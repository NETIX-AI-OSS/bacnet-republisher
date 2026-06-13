use crate::bacnet::point_from_object;
use crate::config::{self, AppConfig, BbmdConfig, DiscoveryBindFailurePolicy, UiTheme};
use crate::log::{LogBuffer, LogLevel};
use crate::model::{
    DeviceObject, DiscoveredDevice, NetworkInterface, PointConfig, PointIdentity, PointSample,
    PointStatus,
};
use crate::network::{interface_choices, ipv4_interfaces};
use crate::ui::{self, ButtonKind, ChipKind, Icon};
use crate::worker::{
    spawn_discovery, spawn_object_scan, spawn_poll_and_publish, spawn_republisher,
    spawn_scan_all_objects, RepublisherLifecycle, WorkerEvent,
};
use chrono::{DateTime, Local, Utc};
use crossbeam_channel::{unbounded, Receiver, Sender};
use iced::widget::{
    checkbox, column, container, pick_list, progress_bar, row, scrollable, text, Column,
};
use iced::{theme, window, Alignment, Element, Font, Length, Size, Subscription, Task, Theme};
use std::collections::{HashMap, VecDeque};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

const LOG_CAPACITY: usize = 500;
const RECENT_SAMPLE_CAPACITY: usize = 100;
const UI_FONT: Font = Font::with_name("Fira Sans");

pub struct BacnetRepublisher {
    config: AppConfig,
    config_path: PathBuf,
    selected_page: Page,
    interfaces: Vec<NetworkInterface>,
    interface_choices: Vec<Ipv4Addr>,
    devices: Vec<DiscoveredDevice>,
    scanned_objects: Vec<DeviceObject>,
    scan_progress: Option<(usize, usize)>,
    recent_samples: VecDeque<PointSample>,
    last_sample_batch: Vec<PointIdentity>,
    point_statuses: HashMap<PointIdentity, PointStatus>,
    status: String,
    status_level: LogLevel,
    settings: SettingsDraft,
    point_editor: PointEditor,
    selected_point: Option<usize>,
    worker_sender: Sender<WorkerEvent>,
    worker_receiver: Receiver<WorkerEvent>,
    logs: LogBuffer,
    working: bool,
    republisher_state: RepublisherLifecycle,
    republisher_stop: Option<Arc<AtomicBool>>,
    // Two-press guard for the destructive "Clear all local objects" action.
    clear_points_armed: bool,
}

#[derive(Debug, Clone)]
pub enum Message {
    SelectPage(Page),
    ThemeSelected(UiTheme),
    DrainWorkerEvents,
    RefreshInterfaces,
    InterfaceSelected(Ipv4Addr),
    Discover,
    ScanObjects(u32),
    ScanAllObjects,
    AddObjectAsPoint(usize),
    PollAndPublish,
    StartRepublisher,
    StopRepublisher,
    ClearLogs,
    SaveSettings,
    DiscoverAllInterfacesChanged(bool),
    BacnetPortChanged(String),
    BroadcastAddressChanged(String),
    DiscoveryWindowChanged(String),
    ApduTimeoutChanged(String),
    PollConcurrencyChanged(String),
    DeviceBackoffMaxChanged(String),
    DiscoveryBindFailurePolicySelected(DiscoveryBindFailurePolicy),
    BbmdEnabledChanged(bool),
    BbmdAddressChanged(String),
    BbmdPortChanged(String),
    BbmdTtlChanged(String),
    MqttHostChanged(String),
    MqttPortChanged(String),
    MqttTlsChanged(bool),
    MqttClientIdChanged(String),
    MqttTopicPrefixChanged(String),
    MqttHealthTopicChanged(String),
    MqttUsernameChanged(String),
    MqttPasswordChanged(String),
    MqttCaCertPathChanged(String),
    MqttClientCertPathChanged(String),
    MqttClientKeyPathChanged(String),
    MqttClientKeyPassphraseChanged(String),
    MqttRememberSecretsChanged(bool),
    MqttRetainChanged(bool),
    MqttKeepAliveChanged(String),
    PointEnabledChanged(bool),
    PointDeviceInstanceChanged(String),
    PointDeviceLabelChanged(String),
    PointObjectTypeChanged(String),
    PointObjectInstanceChanged(String),
    PointPropertyChanged(String),
    PointTagPathChanged(String),
    PointPollIntervalChanged(String),
    SavePoint,
    EditPoint(usize),
    DeletePoint(usize),
    ClearAllPoints,
    NewPoint,
    TogglePoint(usize, bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Overview,
    Discover,
    Points,
    Republish,
    Settings,
    Logs,
}

impl std::fmt::Display for Page {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overview => formatter.write_str("Overview"),
            Self::Discover => formatter.write_str("Discover"),
            Self::Points => formatter.write_str("Points"),
            Self::Republish => formatter.write_str("Republish"),
            Self::Settings => formatter.write_str("Settings"),
            Self::Logs => formatter.write_str("Logs"),
        }
    }
}

#[derive(Debug, Clone)]
struct SettingsDraft {
    bacnet_port: String,
    broadcast_address: String,
    discovery_window_ms: String,
    apdu_timeout_ms: String,
    poll_concurrency: String,
    device_backoff_max_secs: String,
    discovery_bind_failure_policy: DiscoveryBindFailurePolicy,
    bbmd_enabled: bool,
    bbmd_address: String,
    bbmd_port: String,
    bbmd_ttl: String,
    mqtt_host: String,
    mqtt_port: String,
    mqtt_use_tls: bool,
    mqtt_client_id: String,
    mqtt_topic_prefix: String,
    mqtt_health_topic: String,
    mqtt_username: String,
    mqtt_password: String,
    mqtt_ca_cert_path: String,
    mqtt_client_cert_path: String,
    mqtt_client_key_path: String,
    mqtt_client_key_passphrase: String,
    mqtt_remember_secrets: bool,
    mqtt_retain: bool,
    mqtt_keep_alive_secs: String,
}

impl SettingsDraft {
    fn from_config(config: &AppConfig) -> Self {
        let bbmd = config.bacnet.bbmd.clone();
        Self {
            bacnet_port: config.bacnet.port.to_string(),
            broadcast_address: config.bacnet.broadcast_address.to_string(),
            discovery_window_ms: config.bacnet.discovery_window_ms.to_string(),
            apdu_timeout_ms: config.bacnet.apdu_timeout_ms.to_string(),
            poll_concurrency: config.bacnet.poll_concurrency.to_string(),
            device_backoff_max_secs: config.bacnet.device_backoff_max_secs.to_string(),
            discovery_bind_failure_policy: config.bacnet.discovery_bind_failure_policy,
            bbmd_enabled: bbmd.is_some(),
            bbmd_address: bbmd
                .as_ref()
                .map(|value| value.address.to_string())
                .unwrap_or_default(),
            bbmd_port: bbmd
                .as_ref()
                .map(|value| value.port.to_string())
                .unwrap_or_else(|| "47808".to_string()),
            bbmd_ttl: bbmd
                .as_ref()
                .map(|value| value.ttl_secs.to_string())
                .unwrap_or_else(|| "300".to_string()),
            mqtt_host: config.mqtt.host.clone(),
            mqtt_port: config.mqtt.port.to_string(),
            mqtt_use_tls: config.mqtt.use_tls,
            mqtt_client_id: config.mqtt.client_id.clone(),
            mqtt_topic_prefix: config.mqtt.topic_prefix.clone(),
            mqtt_health_topic: config.mqtt.health_topic.clone(),
            mqtt_username: config.mqtt.username.clone().unwrap_or_default(),
            mqtt_password: config.mqtt.password.clone().unwrap_or_default(),
            mqtt_ca_cert_path: config.mqtt.ca_cert_path.clone().unwrap_or_default(),
            mqtt_client_cert_path: config.mqtt.client_cert_path.clone().unwrap_or_default(),
            mqtt_client_key_path: config.mqtt.client_key_path.clone().unwrap_or_default(),
            mqtt_client_key_passphrase: config
                .mqtt
                .client_key_passphrase
                .clone()
                .unwrap_or_default(),
            mqtt_remember_secrets: config.mqtt.remember_secrets,
            mqtt_retain: config.mqtt.retain,
            mqtt_keep_alive_secs: config.mqtt.keep_alive_secs.to_string(),
        }
    }

    fn apply_to(&self, config: &mut AppConfig) -> Result<(), String> {
        config.bacnet.port = parse_u16(&self.bacnet_port, "BACnet port")?;
        config.bacnet.broadcast_address = parse_ipv4(&self.broadcast_address, "broadcast address")?;
        config.bacnet.discovery_window_ms =
            parse_u64(&self.discovery_window_ms, "discovery window")?;
        config.bacnet.apdu_timeout_ms = parse_u64(&self.apdu_timeout_ms, "APDU timeout")?;
        config.bacnet.poll_concurrency =
            usize::try_from(parse_u64(&self.poll_concurrency, "poll concurrency")?)
                .map_err(|_| "Poll concurrency is too large".to_string())?;
        config.bacnet.device_backoff_max_secs =
            parse_u64(&self.device_backoff_max_secs, "device backoff cap")?;
        config.bacnet.discovery_bind_failure_policy = self.discovery_bind_failure_policy;
        config.bacnet.bbmd = if self.bbmd_enabled {
            Some(BbmdConfig {
                address: parse_ipv4(&self.bbmd_address, "BBMD address")?,
                port: parse_u16(&self.bbmd_port, "BBMD port")?,
                ttl_secs: parse_u16(&self.bbmd_ttl, "BBMD TTL")?,
            })
        } else {
            None
        };

        config.mqtt.host = self.mqtt_host.trim().to_string();
        config.mqtt.port = parse_u16(&self.mqtt_port, "MQTT port")?;
        config.mqtt.use_tls = self.mqtt_use_tls;
        config.mqtt.client_id = self.mqtt_client_id.trim().to_string();
        config.mqtt.topic_prefix = self.mqtt_topic_prefix.trim().to_string();
        config.mqtt.health_topic = self.mqtt_health_topic.trim().to_string();
        config.mqtt.username = non_empty_string(&self.mqtt_username);
        config.mqtt.password = non_empty_string(&self.mqtt_password);
        config.mqtt.ca_cert_path = non_empty_string(&self.mqtt_ca_cert_path);
        config.mqtt.client_cert_path = non_empty_string(&self.mqtt_client_cert_path);
        config.mqtt.client_key_path = non_empty_string(&self.mqtt_client_key_path);
        config.mqtt.client_key_passphrase = non_empty_string(&self.mqtt_client_key_passphrase);
        config.mqtt.remember_secrets = self.mqtt_remember_secrets;
        config.mqtt.retain = self.mqtt_retain;
        config.mqtt.keep_alive_secs = parse_u64(&self.mqtt_keep_alive_secs, "MQTT keep-alive")?;
        config.validate()
    }
}

#[derive(Debug, Clone)]
struct PointEditor {
    enabled: bool,
    device_instance: String,
    device_label: String,
    object_type: String,
    object_instance: String,
    property: String,
    tag_path: String,
    poll_interval_secs: String,
}

impl PointEditor {
    fn new() -> Self {
        Self::from_point(&PointConfig::default())
    }

    fn from_point(point: &PointConfig) -> Self {
        Self {
            enabled: point.enabled,
            device_instance: if point.device_instance == 0 {
                String::new()
            } else {
                point.device_instance.to_string()
            },
            device_label: point.device_label.clone(),
            object_type: point.object_type.clone(),
            object_instance: point.object_instance.to_string(),
            property: point.property.clone(),
            tag_path: point.tag_path.clone(),
            poll_interval_secs: point.poll_interval_secs.to_string(),
        }
    }

    fn to_point(&self) -> Result<PointConfig, String> {
        Ok(PointConfig {
            enabled: self.enabled,
            device_instance: parse_u32(&self.device_instance, "device instance")?,
            device_label: self.device_label.trim().to_string(),
            object_type: self.object_type.trim().to_string(),
            object_instance: parse_u32(&self.object_instance, "object instance")?,
            property: self.property.trim().to_string(),
            tag_path: self.tag_path.trim().to_string(),
            poll_interval_secs: parse_u64(&self.poll_interval_secs, "poll interval")?,
        })
    }
}

impl BacnetRepublisher {
    pub fn run() -> iced::Result {
        iced::application(Self::new, Self::update, Self::main_view)
            .title("NETIX BACnet Republisher")
            .subscription(Self::subscription)
            .theme(Self::theme)
            .style(Self::app_style)
            .default_font(UI_FONT)
            .window(window::Settings {
                size: Size::new(1180.0, 760.0),
                min_size: Some(Size::new(920.0, 620.0)),
                icon: window_icon(),
                ..window::Settings::default()
            })
            .antialiasing(true)
            .run()
    }

    fn new() -> (Self, Task<Message>) {
        let (config, config_path, status) = config::load_or_default();
        let interfaces = ipv4_interfaces();
        let interface_choices = interface_choices(&interfaces);
        let (worker_sender, worker_receiver) = unbounded();
        let mut logs = LogBuffer::new(LOG_CAPACITY);
        logs.push(LogLevel::Info, status.clone());

        (
            Self {
                settings: SettingsDraft::from_config(&config),
                point_editor: PointEditor::new(),
                config,
                config_path,
                selected_page: initial_page(),
                interfaces,
                interface_choices,
                devices: Vec::new(),
                scanned_objects: Vec::new(),
                scan_progress: None,
                recent_samples: VecDeque::new(),
                last_sample_batch: Vec::new(),
                point_statuses: HashMap::new(),
                status,
                status_level: LogLevel::Info,
                selected_point: None,
                worker_sender,
                worker_receiver,
                logs,
                working: false,
                republisher_state: RepublisherLifecycle::Stopped,
                republisher_stop: None,
                clear_points_armed: false,
            },
            Task::none(),
        )
    }

    fn theme(&self) -> Theme {
        match self.config.ui.theme {
            UiTheme::Auto => Theme::Dark,
            UiTheme::Light => Theme::Light,
            UiTheme::Dark => Theme::Dark,
        }
    }

    fn app_style(&self, _theme: &Theme) -> theme::Style {
        ui::app_style(self.palette())
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(Duration::from_millis(250)).map(|_| Message::DrainWorkerEvents)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        // Any interaction other than the periodic event drain disarms a pending
        // "clear all" confirmation, so the destructive wipe needs a deliberate
        // double-press and cannot fire after the user has moved on to something else.
        if !matches!(
            message,
            Message::ClearAllPoints | Message::DrainWorkerEvents
        ) {
            self.clear_points_armed = false;
        }
        match message {
            Message::SelectPage(page) => self.selected_page = page,
            Message::ThemeSelected(theme) => {
                self.config.ui.theme = theme;
                self.save_config_with_status();
            }
            Message::DrainWorkerEvents => self.drain_worker_events(),
            Message::RefreshInterfaces => {
                self.interfaces = ipv4_interfaces();
                self.interface_choices = interface_choices(&self.interfaces);
                self.set_status(LogLevel::Info, "Network interfaces refreshed");
            }
            Message::InterfaceSelected(addr) => {
                self.config.bacnet.selected_interface = Some(addr);
                self.config.bacnet.discover_all_interfaces = false;
                self.save_config_with_status();
            }
            Message::Discover => self.start_discovery(),
            Message::ScanObjects(device_instance) => self.start_object_scan(device_instance),
            Message::ScanAllObjects => self.start_scan_all_objects(),
            Message::AddObjectAsPoint(index) => self.add_object_as_point(index),
            Message::PollAndPublish => self.start_poll_and_publish(),
            Message::StartRepublisher => self.start_republisher(),
            Message::StopRepublisher => self.stop_republisher(),
            Message::ClearLogs => self.logs.clear(),
            Message::SaveSettings => self.save_settings(),
            Message::DiscoverAllInterfacesChanged(value) => {
                self.config.bacnet.discover_all_interfaces = value;
                self.save_config_with_status();
            }
            Message::BacnetPortChanged(value) => self.settings.bacnet_port = value,
            Message::BroadcastAddressChanged(value) => self.settings.broadcast_address = value,
            Message::DiscoveryWindowChanged(value) => self.settings.discovery_window_ms = value,
            Message::ApduTimeoutChanged(value) => self.settings.apdu_timeout_ms = value,
            Message::PollConcurrencyChanged(value) => self.settings.poll_concurrency = value,
            Message::DeviceBackoffMaxChanged(value) => {
                self.settings.device_backoff_max_secs = value
            }
            Message::DiscoveryBindFailurePolicySelected(value) => {
                self.settings.discovery_bind_failure_policy = value;
            }
            Message::BbmdEnabledChanged(value) => self.settings.bbmd_enabled = value,
            Message::BbmdAddressChanged(value) => self.settings.bbmd_address = value,
            Message::BbmdPortChanged(value) => self.settings.bbmd_port = value,
            Message::BbmdTtlChanged(value) => self.settings.bbmd_ttl = value,
            Message::MqttHostChanged(value) => self.settings.mqtt_host = value,
            Message::MqttPortChanged(value) => self.settings.mqtt_port = value,
            Message::MqttTlsChanged(value) => self.settings.mqtt_use_tls = value,
            Message::MqttClientIdChanged(value) => self.settings.mqtt_client_id = value,
            Message::MqttTopicPrefixChanged(value) => self.settings.mqtt_topic_prefix = value,
            Message::MqttHealthTopicChanged(value) => self.settings.mqtt_health_topic = value,
            Message::MqttUsernameChanged(value) => self.settings.mqtt_username = value,
            Message::MqttPasswordChanged(value) => self.settings.mqtt_password = value,
            Message::MqttCaCertPathChanged(value) => self.settings.mqtt_ca_cert_path = value,
            Message::MqttClientCertPathChanged(value) => {
                self.settings.mqtt_client_cert_path = value
            }
            Message::MqttClientKeyPathChanged(value) => self.settings.mqtt_client_key_path = value,
            Message::MqttClientKeyPassphraseChanged(value) => {
                self.settings.mqtt_client_key_passphrase = value
            }
            Message::MqttRememberSecretsChanged(value) => {
                self.settings.mqtt_remember_secrets = value
            }
            Message::MqttRetainChanged(value) => self.settings.mqtt_retain = value,
            Message::MqttKeepAliveChanged(value) => self.settings.mqtt_keep_alive_secs = value,
            Message::PointEnabledChanged(value) => self.point_editor.enabled = value,
            Message::PointDeviceInstanceChanged(value) => self.point_editor.device_instance = value,
            Message::PointDeviceLabelChanged(value) => self.point_editor.device_label = value,
            Message::PointObjectTypeChanged(value) => self.point_editor.object_type = value,
            Message::PointObjectInstanceChanged(value) => self.point_editor.object_instance = value,
            Message::PointPropertyChanged(value) => self.point_editor.property = value,
            Message::PointTagPathChanged(value) => self.point_editor.tag_path = value,
            Message::PointPollIntervalChanged(value) => {
                self.point_editor.poll_interval_secs = value
            }
            Message::SavePoint => self.save_point(),
            Message::EditPoint(index) => self.edit_point(index),
            Message::DeletePoint(index) => self.delete_point(index),
            Message::ClearAllPoints => self.clear_all_points(),
            Message::NewPoint => {
                self.selected_point = None;
                self.point_editor = PointEditor::new();
            }
            Message::TogglePoint(index, enabled) => {
                if let Some(point) = self.config.points.get_mut(index) {
                    point.enabled = enabled;
                    self.save_config_with_status();
                }
            }
        }
        Task::none()
    }

    fn main_view(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let sidebar = container(
            column![
                ui::brand(),
                ui::chip(
                    palette,
                    self.republisher_sidebar_label(),
                    self.republisher_chip_kind()
                ),
                iced::widget::rule::horizontal(1),
                self.nav_button(Page::Overview),
                self.nav_button(Page::Discover),
                self.nav_button(Page::Points),
                self.nav_button(Page::Republish),
                self.nav_button(Page::Settings),
                self.nav_button(Page::Logs),
                container(
                    column![
                        ui::eyebrow(palette, "MQTT TARGET"),
                        text(format!(
                            "{}:{}",
                            self.config.mqtt.host, self.config.mqtt.port
                        ))
                        .size(13)
                        .color(palette.text),
                        text(if self.config.mqtt.use_tls {
                            "TLS enabled"
                        } else {
                            "Plain TCP"
                        })
                        .size(12)
                        .color(palette.muted),
                    ]
                    .spacing(4)
                )
                .padding(12)
                .width(Length::Fill)
                .style(move |_| ui::row_style(palette)),
            ]
            .spacing(11)
            .padding(18)
            .width(Length::Fixed(260.0)),
        )
        .height(Length::Fill)
        .style(move |_| ui::sidebar_style(palette));

        let content = match self.selected_page {
            Page::Overview => self.overview_page(),
            Page::Discover => self.discover_page(),
            Page::Points => self.points_page(),
            Page::Republish => self.republish_page(),
            Page::Settings => self.settings_page(),
            Page::Logs => self.logs_page(),
        };

        row![
            sidebar,
            column![content, self.status_bar()].height(Length::Fill)
        ]
        .height(Length::Fill)
        .into()
    }

    fn overview_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let publish_kind = if self.republisher_active() {
            ChipKind::Success
        } else if self.enabled_point_count() == 0 {
            ChipKind::Warning
        } else {
            ChipKind::Accent
        };
        let last_state = self.last_point_state();

        let metrics = row![
            ui::metric(
                palette,
                "Republisher",
                self.republisher_mode_label(),
                format!("{} enabled point(s)", self.enabled_point_count()),
                publish_kind
            ),
            ui::metric(
                palette,
                "Discovery",
                self.devices.len().to_string(),
                format!("{} scanned object(s)", self.scanned_objects.len()),
                ChipKind::Accent
            ),
            ui::metric(
                palette,
                "Samples",
                self.recent_samples.len().to_string(),
                last_state,
                self.overall_point_kind()
            ),
        ]
        .spacing(14);

        let operations = ui::card(
            palette,
            column![
                ui::section_title(palette, "Primary operations"),
                row![
                    ui::action_button(palette, Icon::Discover, "Discover", ButtonKind::Primary)
                        .on_press(Message::Discover),
                    ui::action_button(palette, Icon::Publish, "Poll once", ButtonKind::Secondary)
                        .on_press(Message::PollAndPublish),
                    ui::action_button(
                        palette,
                        Icon::Start,
                        self.republisher_start_label(),
                        ButtonKind::Secondary
                    )
                    .on_press(Message::StartRepublisher),
                    ui::action_button(palette, Icon::Stop, "Stop", ButtonKind::Danger)
                        .on_press(Message::StopRepublisher),
                ]
                .spacing(10)
                .align_y(Alignment::Center),
                row![
                    ui::field_readout(
                        palette,
                        "Topic prefix",
                        self.config.mqtt.topic_prefix.clone()
                    ),
                    ui::field_readout(
                        palette,
                        "Health topic",
                        self.config.mqtt.health_topic.clone()
                    ),
                    ui::field_readout(
                        palette,
                        "BACnet bind",
                        self.config
                            .bacnet
                            .selected_interface
                            .map(|addr| addr.to_string())
                            .unwrap_or_else(|| "Auto / all interfaces".to_string())
                    ),
                ]
                .spacing(16),
            ]
            .spacing(14),
        );

        let recent = ui::card(
            palette,
            column![
                ui::section_title(palette, "Recent activity"),
                self.log_preview(5),
            ]
            .spacing(12),
        );

        self.page_shell(
            "Overview",
            "Operational status, publishing health, and fast actions.",
            column![metrics, operations, recent].spacing(16),
        )
    }

    fn discover_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let selected_bind = self
            .config
            .bacnet
            .selected_interface
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "Auto".to_string());
        let metrics = row![
            ui::metric(
                palette,
                "Devices",
                self.devices.len().to_string(),
                format!("{} interface option(s)", self.interface_choices.len()),
                if self.devices.is_empty() {
                    ChipKind::Neutral
                } else {
                    ChipKind::Success
                }
            ),
            ui::metric(
                palette,
                "Objects",
                self.scanned_objects.len().to_string(),
                "Discovered from object lists",
                if self.scanned_objects.is_empty() {
                    ChipKind::Neutral
                } else {
                    ChipKind::Accent
                }
            ),
            ui::metric(
                palette,
                "Bind",
                selected_bind,
                if self.config.bacnet.discover_all_interfaces {
                    "All interfaces enabled"
                } else {
                    "Single interface"
                },
                ChipKind::Accent
            ),
        ]
        .spacing(14);

        let scan_all_enabled = !self.devices.is_empty() && !self.working;
        let mut scan_all_button =
            ui::action_button(palette, Icon::Points, "Scan all", ButtonKind::Secondary);
        if scan_all_enabled {
            scan_all_button = scan_all_button.on_press(Message::ScanAllObjects);
        }
        let controls = ui::card(
            palette,
            column![
                row![
                    column![
                        ui::section_title(palette, "Discovery command center"),
                        ui::muted(
                            palette,
                            "Find devices first, then scan object lists to seed point configuration."
                        )
                    ]
                    .spacing(4)
                    .width(Length::Fill),
                    ui::chip(
                        palette,
                        if self.working { "Scanning" } else { "Ready" },
                        if self.working {
                            ChipKind::Warning
                        } else {
                            ChipKind::Success
                        }
                    )
                ]
                .spacing(12)
                .align_y(Alignment::Center),
                row![
                    ui::action_button(
                        palette,
                        Icon::Refresh,
                        "Refresh NICs",
                        ButtonKind::Secondary
                    )
                    .on_press(Message::RefreshInterfaces),
                    ui::action_button(palette, Icon::Discover, "Discover", ButtonKind::Primary)
                        .on_press(Message::Discover),
                    scan_all_button,
                ]
                .spacing(10)
                .align_y(Alignment::Center),
                data_row(
                    palette,
                    column![
                        row![
                            checkbox(self.config.bacnet.discover_all_interfaces)
                                .label("All interfaces")
                                .on_toggle(Message::DiscoverAllInterfacesChanged),
                            pick_list(
                                self.interface_choices.clone(),
                                self.config.bacnet.selected_interface,
                                Message::InterfaceSelected
                            )
                            .placeholder("Bind interface"),
                            ui::field_readout(
                                palette,
                                "Bind policy",
                                self.settings.discovery_bind_failure_policy.to_string()
                            ),
                        ]
                        .spacing(12)
                        .align_y(Alignment::Center),
                        ui::muted(
                            palette,
                            if self.config.bacnet.discover_all_interfaces {
                                "All interfaces ignores the bind picker. Select an interface to switch to single-NIC discovery."
                            } else {
                                "Discovery and polling use the selected bind interface."
                            }
                        ),
                    ]
                    .spacing(6),
                ),
            ]
            .spacing(12),
        );

        let mut devices = column![ui::section_title(palette, "Discovered devices")].spacing(8);
        if self.devices.is_empty() {
            devices = devices.push(empty_state(
                palette,
                "No BACnet devices discovered yet.",
                "Run discovery to populate reachable BACnet/IP devices.",
            ));
        } else {
            for device in &self.devices {
                devices = devices.push(data_row(
                    palette,
                    row![
                        readout(palette, "Device", format!("#{}", device.instance), 2),
                        readout(palette, "Address", device.address.clone(), 2),
                        readout(palette, "Vendor", format!("{}", device.vendor_id), 1),
                        ui::action_button(
                            palette,
                            Icon::Points,
                            "Scan objects",
                            ButtonKind::Secondary
                        )
                        .on_press(Message::ScanObjects(device.instance)),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                ));
            }
        }

        let mut objects = column![ui::section_title(palette, "Scanned objects")].spacing(8);
        if self.scanned_objects.is_empty() {
            objects = objects.push(empty_state(
                palette,
                "No objects scanned yet.",
                "Scan a device object list or add points manually.",
            ));
        } else {
            for (index, object) in self.scanned_objects.iter().enumerate() {
                let summary = self.object_summary(object);
                objects = objects.push(data_row(
                    palette,
                    column![
                        row![
                            readout(
                                palette,
                                "Object",
                                format!("{} {}", object.object_type, object.object_instance),
                                2
                            ),
                            readout(palette, "Device", format!("#{}", object.device_instance), 1),
                            ui::action_button(
                                palette,
                                Icon::Save,
                                "Add point",
                                ButtonKind::Primary
                            )
                            .on_press(Message::AddObjectAsPoint(index)),
                        ]
                        .spacing(12)
                        .align_y(Alignment::Center),
                        ui::field_readout(
                            palette,
                            "Preview",
                            if summary.is_empty() {
                                "No metadata returned".to_string()
                            } else {
                                summary
                            }
                        ),
                    ]
                    .spacing(8),
                ));
            }
        }

        let progress: Element<'_, Message> = if let Some((current, total)) = self.scan_progress {
            let frac = if total == 0 {
                0.0
            } else {
                current as f32 / total as f32
            };
            ui::card(
                palette,
                column![
                    text(format!(
                        "Scanning device {} of {} ({}%)",
                        current,
                        total,
                        (frac * 100.0) as u32
                    ))
                    .size(13)
                    .color(palette.text),
                    progress_bar(0.0..=1.0, frac),
                ]
                .spacing(8),
            )
        } else {
            column![].into()
        };

        self.page_shell(
            "Discover",
            "Find BACnet/IP devices and seed point configuration.",
            column![
                metrics,
                controls,
                progress,
                ui::card(palette, devices),
                ui::card(palette, objects)
            ]
            .spacing(16),
        )
    }

    fn points_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let point_metrics = row![
            ui::metric(
                palette,
                "Configured",
                self.config.points.len().to_string(),
                "BACnet objects tracked",
                ChipKind::Accent
            ),
            ui::metric(
                palette,
                "Enabled",
                self.enabled_point_count().to_string(),
                "Included in polling",
                if self.enabled_point_count() == 0 {
                    ChipKind::Warning
                } else {
                    ChipKind::Success
                }
            ),
            ui::metric(
                palette,
                "Point health",
                match self.overall_point_kind() {
                    ChipKind::Success => "OK",
                    ChipKind::Warning => "Attention",
                    ChipKind::Danger => "Error",
                    _ => "No samples",
                },
                self.last_point_state(),
                self.overall_point_kind()
            ),
        ]
        .spacing(14);

        let editor = ui::card(
            palette,
            column![
                ui::section_title(
                    palette,
                    if self.selected_point.is_some() {
                        "Edit point"
                    } else {
                        "New point"
                    }
                ),
                row![
                    ui::labeled_input(
                        palette,
                        "Device instance",
                        "BACnet device id",
                        &self.point_editor.device_instance,
                        Message::PointDeviceInstanceChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Device label",
                        "Topic-friendly name",
                        &self.point_editor.device_label,
                        Message::PointDeviceLabelChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Object type",
                        "analogInput, binaryValue...",
                        &self.point_editor.object_type,
                        Message::PointObjectTypeChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Object instance",
                        "BACnet object id",
                        &self.point_editor.object_instance,
                        Message::PointObjectInstanceChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Property",
                        "presentValue by default",
                        &self.point_editor.property,
                        Message::PointPropertyChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Tag path",
                        "Optional MQTT path",
                        &self.point_editor.tag_path,
                        Message::PointTagPathChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Poll seconds",
                        "Per-point interval",
                        &self.point_editor.poll_interval_secs,
                        Message::PointPollIntervalChanged
                    ),
                ]
                .spacing(12),
                row![
                    checkbox(self.point_editor.enabled)
                        .label("Enabled")
                        .on_toggle(Message::PointEnabledChanged),
                    ui::action_button(palette, Icon::Save, "Save point", ButtonKind::Primary)
                        .on_press(Message::SavePoint),
                    ui::action_button(palette, Icon::Points, "New point", ButtonKind::Secondary)
                        .on_press(Message::NewPoint),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
            ]
            .spacing(12),
        );

        let clear_label = if self.clear_points_armed {
            "Confirm clear"
        } else {
            "Clear all"
        };
        let mut clear_button =
            ui::action_button(palette, Icon::Delete, clear_label, ButtonKind::Danger);
        if !self.config.points.is_empty() || !self.scanned_objects.is_empty() {
            clear_button = clear_button.on_press(Message::ClearAllPoints);
        }
        let mut list = column![row![
            container(ui::section_title(palette, "Configured points")).width(Length::Fill),
            clear_button,
        ]
        .align_y(Alignment::Center)]
        .spacing(8);
        if self.config.points.is_empty() {
            list = list.push(empty_state(
                palette,
                "No points configured.",
                "Add points manually or seed them from a BACnet object scan.",
            ));
        } else {
            for (index, point) in self.config.points.iter().enumerate() {
                let (kind, label) = self.point_status_chip(point);
                let live_value = self.live_value_text(point);
                let last_sampled = self.last_sampled_text(point);
                list = list.push(data_row(
                    palette,
                    column![
                        row![
                            checkbox(point.enabled)
                                .label("")
                                .on_toggle(move |value| Message::TogglePoint(index, value)),
                            column![
                                text(point.display_name()).size(15).color(palette.text),
                                text(format!("Device #{}", point.device_instance))
                                    .size(12)
                                    .color(palette.subtle)
                            ]
                            .spacing(3)
                            .width(Length::Fill),
                            ui::chip(palette, label, kind),
                            ui::action_button(palette, Icon::Edit, "Edit", ButtonKind::Secondary)
                                .on_press(Message::EditPoint(index)),
                            ui::action_button(palette, Icon::Delete, "Delete", ButtonKind::Danger)
                                .on_press(Message::DeletePoint(index)),
                        ]
                        .spacing(10)
                        .align_y(Alignment::Center),
                        row![
                            readout(
                                palette,
                                "Topic path",
                                if point.tag_path.is_empty() {
                                    "(default topic)"
                                } else {
                                    &point.tag_path
                                },
                                3
                            ),
                            readout(palette, "Live value", live_value, 1),
                            readout(palette, "Last sampled", last_sampled, 1),
                            readout(
                                palette,
                                "Poll interval",
                                format!("{}s", point.poll_interval_secs),
                                1
                            ),
                        ]
                        .spacing(12)
                        .align_y(Alignment::Center),
                    ]
                    .spacing(10),
                ));
            }
        }

        self.page_shell(
            "Points",
            "Manage BACnet objects and MQTT tag paths.",
            column![point_metrics, editor, ui::card(palette, list)].spacing(16),
        )
    }

    fn republish_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let publish_metrics = row![
            ui::metric(
                palette,
                "Mode",
                self.republisher_mode_label(),
                self.republisher_mode_hint(),
                self.republisher_chip_kind()
            ),
            ui::metric(
                palette,
                "Enabled points",
                self.enabled_point_count().to_string(),
                "Eligible for MQTT publish",
                if self.enabled_point_count() == 0 {
                    ChipKind::Warning
                } else {
                    ChipKind::Success
                }
            ),
            ui::metric(
                palette,
                "Last update",
                self.last_update_text(),
                format!("{} recent sample(s)", self.recent_samples.len()),
                self.overall_point_kind()
            ),
        ]
        .spacing(14);

        let mut live_points = column![ui::section_title(palette, "Live points")].spacing(8);
        let enabled_points = self
            .config
            .points
            .iter()
            .filter(|point| point.enabled)
            .collect::<Vec<_>>();
        if enabled_points.is_empty() {
            live_points = live_points.push(empty_state(
                palette,
                "No enabled points.",
                "Enable at least one point to show live republished values.",
            ));
        } else {
            for point in enabled_points {
                let (kind, label) = self.point_status_chip(point);
                live_points = live_points.push(data_row(
                    palette,
                    column![
                        row![
                            column![
                                text(point.display_name()).size(15).color(palette.text),
                                text(format!("Device #{}", point.device_instance))
                                    .size(12)
                                    .color(palette.subtle)
                            ]
                            .spacing(3)
                            .width(Length::Fill),
                            ui::chip(palette, label, kind),
                        ]
                        .spacing(10)
                        .align_y(Alignment::Center),
                        row![
                            readout(palette, "Live value", self.live_value_text(point), 1),
                            readout(palette, "Last sampled", self.last_sampled_text(point), 1),
                            readout(
                                palette,
                                "Poll interval",
                                format!("{}s", point.poll_interval_secs),
                                1
                            ),
                        ]
                        .spacing(12)
                        .align_y(Alignment::Center),
                    ]
                    .spacing(10),
                ));
            }
        }

        let mut samples = column![ui::section_title(palette, "Recent samples")].spacing(8);
        if self.recent_samples.is_empty() {
            samples = samples.push(empty_state(
                palette,
                "No point samples published yet.",
                "Use Discover → Scan all to seed points, then poll or start the republisher.",
            ));
        } else {
            for sample in self.recent_samples.iter().rev().take(20) {
                samples = samples.push(data_row(
                    palette,
                    row![
                        readout(palette, "Topic", sample.topic.clone(), 3),
                        readout(palette, "Value", sample.value.to_string(), 1),
                        readout(palette, "Sampled", format_timestamp(sample.timestamp_ms), 1),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                ));
            }
        }

        let summary = ui::card(
            palette,
            column![
                row![
                    ui::section_title(palette, "MQTT target"),
                    ui::chip(
                        palette,
                        self.republisher_mode_label(),
                        self.republisher_chip_kind()
                    )
                ]
                .spacing(10)
                .align_y(Alignment::Center),
                row![
                    ui::field_readout(
                        palette,
                        "Endpoint",
                        format!(
                            "{}:{} ({})",
                            self.config.mqtt.host,
                            self.config.mqtt.port,
                            if self.config.mqtt.use_tls {
                                "TLS"
                            } else {
                                "plain TCP"
                            }
                        )
                    ),
                    ui::field_readout(
                        palette,
                        "Topic prefix",
                        self.config.mqtt.topic_prefix.clone()
                    ),
                    ui::field_readout(
                        palette,
                        "Health topic",
                        self.config.mqtt.health_topic.clone()
                    ),
                ]
                .spacing(16),
                row![
                    ui::action_button(palette, Icon::Publish, "Poll once", ButtonKind::Secondary)
                        .on_press(Message::PollAndPublish),
                    ui::action_button(
                        palette,
                        Icon::Start,
                        self.republisher_start_label(),
                        ButtonKind::Secondary
                    )
                    .on_press(Message::StartRepublisher),
                    ui::action_button(palette, Icon::Stop, "Stop", ButtonKind::Danger)
                        .on_press(Message::StopRepublisher),
                    ui::chip(
                        palette,
                        format!("{} enabled point(s)", self.enabled_point_count()),
                        if self.enabled_point_count() > 0 {
                            ChipKind::Accent
                        } else {
                            ChipKind::Warning
                        }
                    ),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
            ]
            .spacing(10),
        );

        self.page_shell(
            "Republish",
            "Poll configured BACnet points and publish scalar MQTT values.",
            column![
                publish_metrics,
                summary,
                ui::card(palette, live_points),
                ui::card(palette, samples)
            ]
            .spacing(16),
        )
    }

    fn settings_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let settings_summary = ui::card(
            palette,
            column![
                row![
                    column![
                        ui::section_title(palette, "Configuration summary"),
                        ui::muted(
                            palette,
                            "Review transport defaults, save local preferences, and keep secrets opt-in."
                        )
                    ]
                    .spacing(4)
                    .width(Length::Fill),
                    ui::action_button(palette, Icon::Save, "Save settings", ButtonKind::Primary)
                        .on_press(Message::SaveSettings),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
                row![
                    ui::field_readout(
                        palette,
                        "MQTT endpoint",
                        format!(
                            "{}:{} ({})",
                            self.settings.mqtt_host,
                            self.settings.mqtt_port,
                            if self.settings.mqtt_use_tls {
                                "TLS"
                            } else {
                                "plain TCP"
                            }
                        )
                    ),
                    ui::field_readout(
                        palette,
                        "BACnet discovery",
                        format!(
                            "{} ms window · {}",
                            self.settings.discovery_window_ms,
                            self.settings.discovery_bind_failure_policy
                        )
                    ),
                    ui::field_readout(
                        palette,
                        "Secrets",
                        if self.settings.mqtt_remember_secrets {
                            "Remembered locally"
                        } else {
                            "Not persisted"
                        }
                    ),
                ]
                .spacing(16),
            ]
            .spacing(14),
        );

        let bacnet = ui::card(
            palette,
            column![
                ui::section_title(palette, "BACnet/IP"),
                row![
                    ui::labeled_input(
                        palette,
                        "Local bind port",
                        "0 = ephemeral (recommended)",
                        &self.settings.bacnet_port,
                        Message::BacnetPortChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Broadcast address",
                        "Usually 255.255.255.255",
                        &self.settings.broadcast_address,
                        Message::BroadcastAddressChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Discovery window ms",
                        "Minimum 250 ms",
                        &self.settings.discovery_window_ms,
                        Message::DiscoveryWindowChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "APDU timeout ms",
                        "Minimum 250 ms",
                        &self.settings.apdu_timeout_ms,
                        Message::ApduTimeoutChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Poll concurrency",
                        "Devices read in parallel (1-64)",
                        &self.settings.poll_concurrency,
                        Message::PollConcurrencyChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Device backoff cap s",
                        "Max retry delay for failing devices",
                        &self.settings.device_backoff_max_secs,
                        Message::DeviceBackoffMaxChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::field_readout(
                        palette,
                        "Discovery bind errors",
                        "When scanning multiple interfaces"
                    ),
                    pick_list(
                        DiscoveryBindFailurePolicy::ALL.to_vec(),
                        Some(self.settings.discovery_bind_failure_policy),
                        Message::DiscoveryBindFailurePolicySelected
                    ),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
                checkbox(self.settings.bbmd_enabled)
                    .label("Register as foreign device through BBMD")
                    .on_toggle(Message::BbmdEnabledChanged),
                row![
                    ui::labeled_input(
                        palette,
                        "BBMD address",
                        "Foreign device target",
                        &self.settings.bbmd_address,
                        Message::BbmdAddressChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "BBMD port",
                        "Usually 47808",
                        &self.settings.bbmd_port,
                        Message::BbmdPortChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "BBMD TTL",
                        "Seconds",
                        &self.settings.bbmd_ttl,
                        Message::BbmdTtlChanged
                    ),
                ]
                .spacing(12),
            ]
            .spacing(12),
        );

        let mqtt = ui::card(
            palette,
            column![
                ui::section_title(palette, "MQTT"),
                row![
                    ui::labeled_input(
                        palette,
                        "Host",
                        "Broker hostname or IP",
                        &self.settings.mqtt_host,
                        Message::MqttHostChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Port",
                        "8883 TLS / 1883 plain",
                        &self.settings.mqtt_port,
                        Message::MqttPortChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Client ID",
                        "MQTT client identifier",
                        &self.settings.mqtt_client_id,
                        Message::MqttClientIdChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Keep-alive seconds",
                        "Broker heartbeat",
                        &self.settings.mqtt_keep_alive_secs,
                        Message::MqttKeepAliveChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Topic prefix",
                        "Telemetry root",
                        &self.settings.mqtt_topic_prefix,
                        Message::MqttTopicPrefixChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Health topic",
                        "Republisher heartbeat",
                        &self.settings.mqtt_health_topic,
                        Message::MqttHealthTopicChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Username",
                        "Optional broker user",
                        &self.settings.mqtt_username,
                        Message::MqttUsernameChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Password",
                        "Saved only if enabled below",
                        &self.settings.mqtt_password,
                        Message::MqttPasswordChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "CA certificate PEM",
                        "Optional trust anchor",
                        &self.settings.mqtt_ca_cert_path,
                        Message::MqttCaCertPathChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Client certificate PEM",
                        "Mutual TLS certificate",
                        &self.settings.mqtt_client_cert_path,
                        Message::MqttClientCertPathChanged
                    ),
                ]
                .spacing(12),
                row![
                    ui::labeled_input(
                        palette,
                        "Client key PEM",
                        "Required with client cert",
                        &self.settings.mqtt_client_key_path,
                        Message::MqttClientKeyPathChanged
                    ),
                    ui::labeled_input(
                        palette,
                        "Client key passphrase",
                        "Saved only if enabled below",
                        &self.settings.mqtt_client_key_passphrase,
                        Message::MqttClientKeyPassphraseChanged
                    ),
                ]
                .spacing(12),
                row![
                    checkbox(self.settings.mqtt_use_tls)
                        .label("Use TLS")
                        .on_toggle(Message::MqttTlsChanged),
                    checkbox(self.settings.mqtt_retain)
                        .label("Retain telemetry")
                        .on_toggle(Message::MqttRetainChanged),
                    checkbox(self.settings.mqtt_remember_secrets)
                        .label("Remember secrets")
                        .on_toggle(Message::MqttRememberSecretsChanged),
                ]
                .spacing(12),
            ]
            .spacing(12),
        );

        let ui_panel = ui::card(
            palette,
            row![
                ui::field_readout(palette, "Theme", "Local operator preference"),
                pick_list(
                    UiTheme::ALL.to_vec(),
                    Some(self.config.ui.theme),
                    Message::ThemeSelected
                ),
                ui::action_button(palette, Icon::Save, "Save settings", ButtonKind::Primary)
                    .on_press(Message::SaveSettings),
            ]
            .spacing(12)
            .align_y(Alignment::Center),
        );

        self.page_shell(
            "Settings",
            "Configure BACnet transport, MQTT destination, and local preferences.",
            column![settings_summary, bacnet, mqtt, ui_panel].spacing(16),
        )
    }

    fn logs_page(&self) -> Element<'_, Message> {
        let palette = self.palette();
        let newest_log = self
            .logs
            .entries()
            .back()
            .map(|entry| format!("+{}s", entry.elapsed.as_secs()))
            .unwrap_or_else(|| "None".to_string());
        let log_metrics = row![
            ui::metric(
                palette,
                "Entries",
                self.logs.entries().len().to_string(),
                "Buffered app events",
                ChipKind::Accent
            ),
            ui::metric(
                palette,
                "Latest",
                newest_log,
                "Since app launch",
                ChipKind::Neutral
            ),
            ui::metric(
                palette,
                "Status",
                match self.status_chip_kind() {
                    ChipKind::Success => "OK",
                    ChipKind::Warning => "Attention",
                    ChipKind::Danger => "Error",
                    _ => "Info",
                },
                self.status.clone(),
                self.status_chip_kind()
            ),
        ]
        .spacing(14);

        let mut log_list = column![].spacing(6);
        for entry in self.logs.entries() {
            log_list = log_list.push(self.log_row(
                entry.sequence,
                entry.elapsed.as_secs(),
                entry.level,
                &entry.message,
            ));
        }

        self.page_shell(
            "Logs",
            "Recent app, BACnet, and MQTT activity.",
            column![
                log_metrics,
                row![
                    ui::action_button(palette, Icon::Delete, "Clear logs", ButtonKind::Danger)
                        .on_press(Message::ClearLogs),
                    ui::chip(palette, self.status.clone(), self.status_chip_kind())
                ]
                .spacing(12)
                .align_y(Alignment::Center),
                ui::card(palette, log_list)
            ]
            .spacing(16),
        )
    }

    fn page_shell<'a>(
        &'a self,
        title: &'a str,
        subtitle: &'a str,
        body: Column<'a, Message>,
    ) -> Element<'a, Message> {
        let palette = self.palette();
        let mut state_chips = row![ui::chip(
            palette,
            if self.working { "Working" } else { "Ready" },
            if self.working {
                ChipKind::Warning
            } else {
                ChipKind::Success
            }
        )]
        .spacing(8);
        if self.republisher_active()
            || matches!(self.republisher_state, RepublisherLifecycle::Failed(_))
        {
            state_chips = state_chips.push(ui::chip(
                palette,
                self.republisher_mode_label(),
                self.republisher_chip_kind(),
            ));
        }
        let header = row![
            column![
                ui::eyebrow(palette, "BACNET REPUBLISHER"),
                text(title).size(30).color(palette.text),
                text(subtitle).size(15).color(palette.muted)
            ]
            .spacing(4)
            .width(Length::Fill),
            state_chips
        ]
        .spacing(16)
        .align_y(Alignment::Center);
        container(scrollable(column![header, body].spacing(18).padding(24)))
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    }

    fn nav_button(&self, page: Page) -> Element<'_, Message> {
        let palette = self.palette();
        ui::nav_button(
            palette,
            page_icon(page),
            page.to_string(),
            self.selected_page == page,
        )
        .on_press(Message::SelectPage(page))
        .into()
    }

    fn start_discovery(&mut self) {
        self.working = true;
        self.devices.clear();
        self.scanned_objects.clear();
        spawn_discovery(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.interfaces.clone(),
        );
    }

    fn start_object_scan(&mut self, device_instance: u32) {
        self.working = true;
        self.scanned_objects.clear();
        spawn_object_scan(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.interfaces.clone(),
            device_instance,
        );
    }

    fn start_scan_all_objects(&mut self) {
        if self.working {
            self.set_status(
                LogLevel::Warning,
                "Another BACnet operation is already running",
            );
            return;
        }
        if self.devices.is_empty() {
            self.set_status(
                LogLevel::Warning,
                "Discover devices before scanning all object lists",
            );
            return;
        }
        self.working = true;
        self.scanned_objects.clear();
        self.scan_progress = Some((0, self.devices.len()));
        spawn_scan_all_objects(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.interfaces.clone(),
            self.devices.clone(),
            self.config.points.clone(),
        );
    }

    fn start_poll_and_publish(&mut self) {
        if self.republisher_active() {
            self.set_status(
                LogLevel::Warning,
                "Stop the continuous republisher before running Poll once",
            );
            return;
        }
        if let Err(error) = self.config.validate() {
            self.set_status(LogLevel::Error, error);
            return;
        }
        if self.enabled_point_count() == 0 {
            self.set_status(
                LogLevel::Warning,
                "Enable at least one point before publishing",
            );
            return;
        }
        self.working = true;
        spawn_poll_and_publish(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.interfaces.clone(),
            self.config.mqtt.clone(),
            self.config.points.clone(),
        );
    }

    fn start_republisher(&mut self) {
        if self.republisher_active() {
            self.set_status(
                LogLevel::Warning,
                "Continuous republisher is already running",
            );
            return;
        }
        if let Err(error) = self.config.validate() {
            self.set_status(LogLevel::Error, error);
            return;
        }
        if self.enabled_point_count() == 0 {
            self.set_status(
                LogLevel::Warning,
                "Enable at least one point before publishing",
            );
            return;
        }

        let stop = Arc::new(AtomicBool::new(false));
        self.republisher_stop = Some(Arc::clone(&stop));
        self.republisher_state = RepublisherLifecycle::Starting;
        spawn_republisher(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.interfaces.clone(),
            self.config.mqtt.clone(),
            self.config.points.clone(),
            stop,
        );
    }

    fn stop_republisher(&mut self) {
        if let Some(stop) = &self.republisher_stop {
            stop.store(true, Ordering::Relaxed);
            self.republisher_state = RepublisherLifecycle::Stopping;
            self.set_status(LogLevel::Info, "Stopping continuous republisher");
        } else {
            self.set_status(LogLevel::Warning, "Continuous republisher is not running");
        }
    }

    fn add_object_as_point(&mut self, index: usize) {
        let Some(object) = self.scanned_objects.get(index) else {
            return;
        };
        let point = point_from_object(object, None);
        self.point_editor = PointEditor::from_point(&point);
        self.selected_point = None;
        self.selected_page = Page::Points;
    }

    fn save_point(&mut self) {
        match self.point_editor.to_point() {
            Ok(point) => {
                if let Some(index) = self.selected_point {
                    if let Some(existing) = self.config.points.get_mut(index) {
                        *existing = point;
                    }
                } else {
                    self.config.points.push(point);
                    self.selected_point = self.config.points.len().checked_sub(1);
                }
                self.save_config_with_status();
            }
            Err(error) => self.set_status(LogLevel::Error, error),
        }
    }

    fn edit_point(&mut self, index: usize) {
        if let Some(point) = self.config.points.get(index) {
            self.selected_point = Some(index);
            self.point_editor = PointEditor::from_point(point);
        }
    }

    fn delete_point(&mut self, index: usize) {
        if index < self.config.points.len() {
            self.config.points.remove(index);
            self.selected_point = None;
            self.point_editor = PointEditor::new();
            self.save_config_with_status();
        }
    }

    /// Clear every locally-held object: the configured/republished points and the
    /// cached object-scan results. Used to drop a stale points table (e.g. after the
    /// upstream BACnet identities changed) before a fresh Discover. Two-press guarded:
    /// the first press arms the confirmation, the second performs the wipe.
    fn clear_all_points(&mut self) {
        let points = self.config.points.len();
        let objects = self.scanned_objects.len();
        if points == 0 && objects == 0 {
            self.clear_points_armed = false;
            self.set_status(LogLevel::Info, "No local objects to clear.");
            return;
        }
        if !self.clear_points_armed {
            self.clear_points_armed = true;
            self.set_status(
                LogLevel::Warning,
                format!(
                    "Press 'Confirm clear' again to remove {} configured point(s) and \
                     {objects} scanned object(s). Any other action cancels.",
                    points
                ),
            );
            return;
        }
        self.config.points.clear();
        self.scanned_objects.clear();
        self.point_statuses.clear();
        self.last_sample_batch.clear();
        self.selected_point = None;
        self.point_editor = PointEditor::new();
        self.clear_points_armed = false;
        self.save_config_with_status();
        self.set_status(
            LogLevel::Info,
            format!(
                "Cleared {points} configured point(s) and {objects} scanned object(s). \
                 Run Discover to repopulate from the current devices."
            ),
        );
    }

    fn save_settings(&mut self) {
        let mut next = self.config.clone();
        match self.settings.apply_to(&mut next) {
            Ok(()) => {
                self.config = next;
                self.save_config_with_status();
            }
            Err(error) => self.set_status(LogLevel::Error, error),
        }
    }

    fn save_config_with_status(&mut self) {
        match config::save_to_path(&self.config_path, &self.config) {
            Ok(()) => self.set_status(
                LogLevel::Info,
                format!("Configuration saved to {}", self.config_path.display()),
            ),
            Err(error) => {
                self.set_status(LogLevel::Error, format!("Config save failed: {error:#}"))
            }
        }
    }

    fn drain_worker_events(&mut self) {
        let events = self.worker_receiver.try_iter().collect::<Vec<_>>();
        for event in events {
            match event {
                WorkerEvent::Log(level, message) => self.set_status(level, message),
                WorkerEvent::Devices(devices) => self.devices = devices,
                WorkerEvent::Objects(objects) => self.scanned_objects = objects,
                WorkerEvent::ScanProgress { current, total } => {
                    self.scan_progress = Some((current, total));
                }
                WorkerEvent::BulkTagImport(outcome) => {
                    self.devices = outcome.devices;
                    self.scanned_objects = outcome.scanned_objects;
                    self.config.points = outcome.points;
                    self.save_config_with_status();
                }
                WorkerEvent::Samples(samples) => self.record_samples(samples),
                WorkerEvent::Failures(failures) => {
                    for failure in failures {
                        self.point_statuses
                            .entry(PointIdentity::from_point(&failure.point))
                            .or_default()
                            .record_read_failure(failure.error);
                    }
                }
                WorkerEvent::PublishStatus(stats) => {
                    let last_error = stats
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "MQTT publish failed".to_string());
                    for identity in &self.last_sample_batch {
                        let status = self.point_statuses.entry(identity.clone()).or_default();
                        if stats.failed == 0 {
                            status.record_publish_success();
                        } else {
                            status.record_publish_failure(last_error.clone());
                        }
                    }
                    self.set_status(
                        if stats.failed == 0 {
                            LogLevel::Info
                        } else {
                            LogLevel::Warning
                        },
                        format!(
                            "MQTT publish complete: {} published, {} failed",
                            stats.published, stats.failed
                        ),
                    );
                }
                WorkerEvent::RepublisherLifecycle(state) => self.apply_republisher_lifecycle(state),
                WorkerEvent::Finished(message) => {
                    self.working = false;
                    self.scan_progress = None;
                    self.set_status(LogLevel::Info, message);
                }
            }
        }
    }

    fn apply_republisher_lifecycle(&mut self, state: RepublisherLifecycle) {
        if matches!(self.republisher_state, RepublisherLifecycle::Stopping)
            && matches!(
                state,
                RepublisherLifecycle::Starting | RepublisherLifecycle::Running
            )
        {
            return;
        }
        match &state {
            RepublisherLifecycle::Starting => {
                self.set_status(LogLevel::Info, "Starting continuous republisher");
            }
            RepublisherLifecycle::Running => {
                self.set_status(LogLevel::Info, "Continuous republisher active");
            }
            RepublisherLifecycle::Stopping => {
                self.set_status(LogLevel::Info, "Stopping continuous republisher");
            }
            RepublisherLifecycle::Stopped => {
                self.republisher_stop = None;
                self.set_status(LogLevel::Info, "Continuous republisher stopped");
            }
            RepublisherLifecycle::Failed(reason) => {
                self.republisher_stop = None;
                self.set_status(
                    LogLevel::Error,
                    format!("Continuous republisher failed: {reason}"),
                );
            }
        }
        self.republisher_state = state;
    }

    fn record_samples(&mut self, samples: Vec<PointSample>) {
        self.last_sample_batch.clear();
        for sample in samples {
            let identity = PointIdentity::from_point(&sample.point);
            self.point_statuses
                .entry(identity.clone())
                .or_default()
                .record_sample(&sample);
            self.recent_samples.push_back(sample);
            self.last_sample_batch.push(identity);
        }
        while self.recent_samples.len() > RECENT_SAMPLE_CAPACITY {
            self.recent_samples.pop_front();
        }
    }

    fn live_value_text(&self, point: &PointConfig) -> String {
        let Some(status) = self.point_statuses.get(&PointIdentity::from_point(point)) else {
            return "—".to_string();
        };
        match &status.last_value {
            Some(value) => value.to_string(),
            None => "—".to_string(),
        }
    }

    fn last_sampled_text(&self, point: &PointConfig) -> String {
        self.point_statuses
            .get(&PointIdentity::from_point(point))
            .and_then(|status| status.last_sample_ms)
            .map(format_timestamp)
            .unwrap_or_else(|| "No sample".to_string())
    }

    fn point_status_chip(&self, point: &PointConfig) -> (ChipKind, String) {
        let Some(status) = self.point_statuses.get(&PointIdentity::from_point(point)) else {
            return (ChipKind::Neutral, "No sample".to_string());
        };
        if status.last_error.is_some() {
            return (ChipKind::Danger, "Read error".to_string());
        }
        if status.last_publish_error.is_some() {
            return (ChipKind::Warning, "Publish error".to_string());
        }
        if status.stale {
            return (ChipKind::Warning, "Stale".to_string());
        }
        if let Some(timestamp) = status.last_sample_ms {
            return (
                ChipKind::Success,
                format!("OK {}", format_timestamp(timestamp)),
            );
        }
        (ChipKind::Success, "OK".to_string())
    }

    fn overall_point_kind(&self) -> ChipKind {
        if self
            .point_statuses
            .values()
            .any(|status| status.last_error.is_some())
        {
            ChipKind::Danger
        } else if self
            .point_statuses
            .values()
            .any(|status| status.stale || status.last_publish_error.is_some())
        {
            ChipKind::Warning
        } else if self.recent_samples.is_empty() {
            ChipKind::Neutral
        } else {
            ChipKind::Success
        }
    }

    fn last_point_state(&self) -> String {
        if let Some(sample) = self.recent_samples.back() {
            format!(
                "{} at {}",
                sample.value,
                format_timestamp(sample.timestamp_ms)
            )
        } else if self.point_statuses.is_empty() {
            "No samples recorded".to_string()
        } else {
            format!("{} tracked point state(s)", self.point_statuses.len())
        }
    }

    fn object_summary(&self, object: &DeviceObject) -> String {
        let mut parts = Vec::new();
        if let Some(name) = &object.object_name {
            parts.push(name.clone());
        }
        if let Some(description) = &object.description {
            parts.push(description.clone());
        }
        if let Some(units) = &object.units {
            parts.push(format!("units {units}"));
        }
        if let Some(value) = &object.present_value {
            parts.push(format!("present {value}"));
        }
        parts.join(" | ")
    }

    fn enabled_point_count(&self) -> usize {
        self.config
            .points
            .iter()
            .filter(|point| point.enabled)
            .count()
    }

    fn republisher_active(&self) -> bool {
        matches!(
            self.republisher_state,
            RepublisherLifecycle::Starting
                | RepublisherLifecycle::Running
                | RepublisherLifecycle::Stopping
        )
    }

    fn republisher_sidebar_label(&self) -> &'static str {
        match self.republisher_state {
            RepublisherLifecycle::Starting => "STARTING",
            RepublisherLifecycle::Running => "LIVE REPUBLISH",
            RepublisherLifecycle::Stopping => "STOPPING",
            RepublisherLifecycle::Stopped => "STANDBY",
            RepublisherLifecycle::Failed(_) => "FAILED",
        }
    }

    fn republisher_mode_label(&self) -> String {
        match &self.republisher_state {
            RepublisherLifecycle::Starting => "Starting".to_string(),
            RepublisherLifecycle::Running => "Running".to_string(),
            RepublisherLifecycle::Stopping => "Stopping".to_string(),
            RepublisherLifecycle::Stopped => "Stopped".to_string(),
            RepublisherLifecycle::Failed(_) => "Failed".to_string(),
        }
    }

    fn republisher_mode_hint(&self) -> String {
        match &self.republisher_state {
            RepublisherLifecycle::Starting => "Preparing MQTT and BACnet clients".to_string(),
            RepublisherLifecycle::Running => "Continuous republisher active".to_string(),
            RepublisherLifecycle::Stopping => "Waiting for current loop to exit".to_string(),
            RepublisherLifecycle::Stopped => "Poll once or start loop".to_string(),
            RepublisherLifecycle::Failed(reason) => reason.clone(),
        }
    }

    fn republisher_start_label(&self) -> &'static str {
        match self.republisher_state {
            RepublisherLifecycle::Starting => "Starting",
            RepublisherLifecycle::Running => "Running",
            RepublisherLifecycle::Stopping => "Stopping",
            RepublisherLifecycle::Stopped => "Start republisher",
            RepublisherLifecycle::Failed(_) => "Restart republisher",
        }
    }

    fn republisher_chip_kind(&self) -> ChipKind {
        match self.republisher_state {
            RepublisherLifecycle::Starting => ChipKind::Warning,
            RepublisherLifecycle::Running => ChipKind::Success,
            RepublisherLifecycle::Stopping => ChipKind::Warning,
            RepublisherLifecycle::Stopped => ChipKind::Neutral,
            RepublisherLifecycle::Failed(_) => ChipKind::Danger,
        }
    }

    fn last_update_text(&self) -> String {
        self.recent_samples
            .back()
            .map(|sample| format_timestamp(sample.timestamp_ms))
            .unwrap_or_else(|| "No samples".to_string())
    }

    fn set_status(&mut self, level: LogLevel, message: impl Into<String>) {
        let message = message.into();
        self.status = message.clone();
        self.status_level = level;
        self.logs.push(level, message);
    }

    fn status_bar(&self) -> Element<'_, Message> {
        let palette = self.palette();
        container(
            row![
                ui::chip(palette, self.status.clone(), self.status_chip_kind()),
                text(format!(
                    "{} pts · {} devices",
                    self.config.points.len(),
                    self.devices.len()
                ))
                .size(13)
                .color(palette.muted)
                .width(Length::Fill),
                text(format!(
                    "Config: {}",
                    compact_config_path(&self.config_path)
                ))
                .size(12)
                .color(palette.subtle),
            ]
            .spacing(14)
            .padding(12)
            .align_y(Alignment::Center),
        )
        .width(Length::Fill)
        .style(move |_| ui::status_bar_style(palette))
        .into()
    }

    fn log_preview(&self, take: usize) -> Element<'_, Message> {
        let palette = self.palette();
        let mut logs = column![].spacing(6);
        if self.logs.entries().is_empty() {
            logs = logs.push(empty_state(
                palette,
                "No activity yet.",
                "App, BACnet, and MQTT events will appear here.",
            ));
        } else {
            for entry in self.logs.entries().iter().rev().take(take) {
                logs = logs.push(self.log_row(
                    entry.sequence,
                    entry.elapsed.as_secs(),
                    entry.level,
                    &entry.message,
                ));
            }
        }
        logs.into()
    }

    fn log_row<'a>(
        &'a self,
        sequence: u64,
        elapsed_secs: u64,
        level: LogLevel,
        message: &'a str,
    ) -> Element<'a, Message> {
        let palette = self.palette();
        data_row(
            palette,
            row![
                ui::chip(
                    palette,
                    level.to_string(),
                    match level {
                        LogLevel::Info => ChipKind::Accent,
                        LogLevel::Warning => ChipKind::Warning,
                        LogLevel::Error => ChipKind::Danger,
                    }
                ),
                text(format!("#{:04}", sequence))
                    .size(12)
                    .color(palette.subtle),
                text(format!("+{}s", elapsed_secs))
                    .size(12)
                    .color(palette.subtle),
                text(message.to_string())
                    .size(13)
                    .color(palette.text)
                    .width(Length::Fill),
            ]
            .spacing(10)
            .align_y(Alignment::Center),
        )
    }

    fn status_chip_kind(&self) -> ChipKind {
        match self.status_level {
            LogLevel::Error => ChipKind::Danger,
            LogLevel::Warning => ChipKind::Warning,
            LogLevel::Info => ChipKind::Accent,
        }
    }

    fn palette(&self) -> ui::Palette {
        ui::palette(self.config.ui.theme)
    }
}

#[cfg(test)]
mod sample_state_tests {
    use super::*;
    use crate::model::TelemetryValue;

    fn app_for_tests() -> BacnetRepublisher {
        let config = AppConfig::default();
        let (worker_sender, worker_receiver) = unbounded();
        BacnetRepublisher {
            settings: SettingsDraft::from_config(&config),
            point_editor: PointEditor::new(),
            config,
            config_path: PathBuf::from("test-config.toml"),
            selected_page: Page::Overview,
            interfaces: Vec::new(),
            interface_choices: Vec::new(),
            devices: Vec::new(),
            scanned_objects: Vec::new(),
            scan_progress: None,
            recent_samples: VecDeque::new(),
            last_sample_batch: Vec::new(),
            point_statuses: HashMap::new(),
            status: String::new(),
            status_level: LogLevel::Info,
            selected_point: None,
            worker_sender,
            worker_receiver,
            logs: LogBuffer::new(LOG_CAPACITY),
            working: false,
            republisher_state: RepublisherLifecycle::Stopped,
            republisher_stop: None,
            clear_points_armed: false,
        }
    }

    fn point(object_instance: u32) -> PointConfig {
        PointConfig {
            device_instance: 100,
            object_instance,
            ..PointConfig::default()
        }
    }

    fn sample(point: PointConfig, value: f64, timestamp_ms: i64) -> PointSample {
        PointSample {
            topic: format!(
                "Netix/Site/device_100/analog_input_{}/present_value",
                point.object_instance
            ),
            point,
            value: TelemetryValue::Number(value),
            timestamp_ms,
        }
    }

    #[test]
    fn clear_all_points_requires_two_presses_and_any_other_action_cancels() {
        let mut app = app_for_tests();
        app.config_path =
            std::env::temp_dir().join(format!("brp-clear-test-{}.toml", std::process::id()));
        app.config.points = vec![point(1), point(2), point(3)];

        // First press only arms the confirmation; nothing is removed yet.
        let _ = app.update(Message::ClearAllPoints);
        assert!(app.clear_points_armed);
        assert_eq!(app.config.points.len(), 3);

        // Any unrelated interaction disarms the pending confirmation...
        let _ = app.update(Message::SelectPage(Page::Points));
        assert!(!app.clear_points_armed);
        // ...so a subsequent lone press just re-arms rather than wiping.
        let _ = app.update(Message::ClearAllPoints);
        assert!(app.clear_points_armed);
        assert_eq!(app.config.points.len(), 3);

        // The periodic event drain must NOT disarm (it fires every 250ms).
        let _ = app.update(Message::DrainWorkerEvents);
        assert!(app.clear_points_armed);

        // Second deliberate press performs the wipe and resets the guard.
        let _ = app.update(Message::ClearAllPoints);
        assert!(app.config.points.is_empty());
        assert!(app.scanned_objects.is_empty());
        assert!(!app.clear_points_armed);

        let _ = std::fs::remove_file(&app.config_path);
    }

    #[test]
    fn record_samples_keeps_latest_value_for_each_point() {
        let mut app = app_for_tests();
        let point_a = point(1);
        let point_b = point(2);

        app.record_samples(vec![sample(point_a.clone(), 10.0, 100)]);
        app.record_samples(vec![sample(point_b.clone(), 20.0, 200)]);

        let identity_a = PointIdentity::from_point(&point_a);
        let identity_b = PointIdentity::from_point(&point_b);
        assert_eq!(
            app.point_statuses
                .get(&identity_a)
                .and_then(|status| status.last_value.as_ref()),
            Some(&TelemetryValue::Number(10.0))
        );
        assert_eq!(
            app.point_statuses
                .get(&identity_b)
                .and_then(|status| status.last_value.as_ref()),
            Some(&TelemetryValue::Number(20.0))
        );
        assert_eq!(app.recent_samples.len(), 2);
        assert_eq!(app.last_sample_batch, vec![identity_b]);
    }

    #[test]
    fn record_samples_caps_recent_history() {
        let mut app = app_for_tests();

        for index in 0..(RECENT_SAMPLE_CAPACITY + 3) {
            let point = point(index as u32 + 1);
            app.record_samples(vec![sample(point, index as f64, index as i64)]);
        }

        assert_eq!(app.recent_samples.len(), RECENT_SAMPLE_CAPACITY);
        assert_eq!(
            app.recent_samples.front().map(|sample| sample.timestamp_ms),
            Some(3)
        );
        assert_eq!(
            app.recent_samples.back().map(|sample| sample.timestamp_ms),
            Some((RECENT_SAMPLE_CAPACITY + 2) as i64)
        );
    }
}

fn readout<'a>(
    palette: ui::Palette,
    label: impl Into<String>,
    value: impl Into<String>,
    portion: u16,
) -> Element<'a, Message> {
    container(ui::field_readout(palette, label, value))
        .width(Length::FillPortion(portion))
        .into()
}

fn data_row<'a>(
    palette: ui::Palette,
    content: impl Into<Element<'a, Message>>,
) -> Element<'a, Message> {
    container(content)
        .padding(12)
        .width(Length::Fill)
        .style(move |_| ui::row_style(palette))
        .into()
}

fn empty_state<'a>(
    palette: ui::Palette,
    title: impl Into<String>,
    detail: impl Into<String>,
) -> Element<'a, Message> {
    container(
        column![
            text(title.into()).size(15).color(palette.text),
            text(detail.into()).size(13).color(palette.muted)
        ]
        .spacing(4),
    )
    .padding(14)
    .width(Length::Fill)
    .style(move |_| ui::row_style(palette))
    .into()
}

fn window_icon() -> Option<window::Icon> {
    window::icon::from_file_data(include_bytes!("../assets/app-icon.png"), None).ok()
}

fn page_icon(page: Page) -> Icon {
    match page {
        Page::Overview => Icon::Overview,
        Page::Discover => Icon::Discover,
        Page::Points => Icon::Points,
        Page::Republish => Icon::Publish,
        Page::Settings => Icon::Settings,
        Page::Logs => Icon::Logs,
    }
}

fn initial_page() -> Page {
    let Ok(value) = std::env::var("BACNET_REPUBLISHER_INITIAL_PAGE") else {
        return Page::Overview;
    };

    match value.trim().to_ascii_lowercase().as_str() {
        "discover" => Page::Discover,
        "points" => Page::Points,
        "republish" | "publish" => Page::Republish,
        "settings" => Page::Settings,
        "logs" => Page::Logs,
        _ => Page::Overview,
    }
}

fn format_timestamp(timestamp_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .map(|timestamp| {
            timestamp
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| timestamp_ms.to_string())
}

fn compact_config_path(path: &Path) -> String {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("config.toml");
    let parent_name = path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|value| value.to_str());

    match parent_name {
        Some(parent) => format!("{parent}/{file_name}"),
        None => file_name.to_string(),
    }
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_u16(value: &str, label: &str) -> Result<u16, String> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("{label} must be a number between 0 and 65535"))
}

fn parse_u32(value: &str, label: &str) -> Result<u32, String> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("{label} must be a non-negative number"))
}

fn parse_u64(value: &str, label: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{label} must be a non-negative number"))
}

fn parse_ipv4(value: &str, label: &str) -> Result<Ipv4Addr, String> {
    value
        .trim()
        .parse::<Ipv4Addr>()
        .map_err(|_| format!("{label} must be an IPv4 address"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- compact_config_path ---

    #[test]
    fn compact_config_path_returns_parent_and_filename() {
        let path = Path::new("/home/user/.config/bacnet/config.toml");
        assert_eq!(compact_config_path(path), "bacnet/config.toml");
    }

    #[test]
    fn compact_config_path_root_fallback() {
        // If there is no meaningful parent name, just return the filename.
        let path = Path::new("config.toml");
        assert_eq!(compact_config_path(path), "config.toml");
    }

    // --- format_timestamp ---

    #[test]
    fn format_timestamp_valid_millis() {
        // 2024-01-15 00:00:00 UTC = 1705276800000 ms
        let result = format_timestamp(1_705_276_800_000);
        // The string format is "%Y-%m-%d %H:%M:%S" in local time; just verify it
        // is non-empty and contains a date-like structure.
        assert!(result.contains('-'), "expected date separator: {result}");
        assert!(result.contains(':'), "expected time separator: {result}");
    }

    #[test]
    fn format_timestamp_negative_millis_returns_pre_epoch_date() {
        // chrono's from_timestamp_millis accepts negative values (pre-epoch).
        // The result should be a formatted date string, not the raw integer.
        let result = format_timestamp(-1);
        assert!(
            result.contains('-'),
            "expected a date string, got: {result}"
        );
        assert!(
            result.contains(':'),
            "expected a time string, got: {result}"
        );
    }

    // --- non_empty_string ---

    #[test]
    fn non_empty_string_returns_some_for_non_blank() {
        assert_eq!(non_empty_string("hello"), Some("hello".to_string()));
    }

    #[test]
    fn non_empty_string_trims_whitespace() {
        assert_eq!(non_empty_string("  hi  "), Some("hi".to_string()));
    }

    #[test]
    fn non_empty_string_returns_none_for_blank() {
        assert_eq!(non_empty_string(""), None);
        assert_eq!(non_empty_string("   "), None);
    }

    // --- parse_u16 ---

    #[test]
    fn parse_u16_valid() {
        assert_eq!(parse_u16("8883", "port"), Ok(8883u16));
    }

    #[test]
    fn parse_u16_trims_whitespace() {
        assert_eq!(parse_u16("  47808  ", "port"), Ok(47808u16));
    }

    #[test]
    fn parse_u16_invalid_returns_error() {
        assert!(parse_u16("99999", "port").is_err());
        assert!(parse_u16("abc", "port").is_err());
    }

    // --- parse_u32 ---

    #[test]
    fn parse_u32_valid() {
        assert_eq!(parse_u32("12345", "instance"), Ok(12345u32));
    }

    #[test]
    fn parse_u32_invalid_returns_error() {
        assert!(parse_u32("-1", "instance").is_err());
    }

    // --- parse_u64 ---

    #[test]
    fn parse_u64_valid() {
        assert_eq!(parse_u64("3000", "window"), Ok(3000u64));
    }

    #[test]
    fn parse_u64_invalid_returns_error() {
        assert!(parse_u64("not_a_number", "window").is_err());
    }

    // --- parse_ipv4 ---

    #[test]
    fn parse_ipv4_valid() {
        use std::net::Ipv4Addr;
        assert_eq!(
            parse_ipv4("192.168.1.1", "addr"),
            Ok(Ipv4Addr::new(192, 168, 1, 1))
        );
    }

    #[test]
    fn parse_ipv4_broadcast() {
        use std::net::Ipv4Addr;
        assert_eq!(
            parse_ipv4("255.255.255.255", "addr"),
            Ok(Ipv4Addr::BROADCAST)
        );
    }

    #[test]
    fn parse_ipv4_invalid_returns_error() {
        assert!(parse_ipv4("not.an.ip", "addr").is_err());
        assert!(parse_ipv4("256.0.0.1", "addr").is_err());
    }

    // --- initial_page ---
    //
    // These tests mutate the process environment, which is shared across
    // Rust's parallel test threads. We serialize all three under a single
    // Mutex so they cannot interfere with each other.

    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn initial_page_defaults_to_overview_when_env_unset() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BACNET_REPUBLISHER_INITIAL_PAGE");
        assert_eq!(initial_page(), Page::Overview);
    }

    #[test]
    fn initial_page_parses_known_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        for (input, expected) in [
            ("discover", Page::Discover),
            ("DISCOVER", Page::Discover),
            ("points", Page::Points),
            ("republish", Page::Republish),
            ("publish", Page::Republish),
            ("settings", Page::Settings),
            ("logs", Page::Logs),
        ] {
            std::env::set_var("BACNET_REPUBLISHER_INITIAL_PAGE", input);
            assert_eq!(initial_page(), expected, "input: {input}");
        }
        std::env::remove_var("BACNET_REPUBLISHER_INITIAL_PAGE");
    }

    #[test]
    fn initial_page_unknown_value_falls_back_to_overview() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("BACNET_REPUBLISHER_INITIAL_PAGE", "unknown_page");
        assert_eq!(initial_page(), Page::Overview);
        std::env::remove_var("BACNET_REPUBLISHER_INITIAL_PAGE");
    }
}
