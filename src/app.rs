use crate::bacnet::point_from_object;
use crate::config::{self, AppConfig, BbmdConfig, UiTheme};
use crate::log::{LogBuffer, LogLevel};
use crate::model::{
    DeviceObject, DiscoveredDevice, NetworkInterface, PointConfig, PointIdentity, PointSample,
    PointStatus,
};
use crate::network::{interface_choices, ipv4_interfaces};
use crate::worker::{
    spawn_discovery, spawn_object_scan, spawn_poll_and_publish, spawn_republisher, WorkerEvent,
};
use crossbeam_channel::{unbounded, Receiver, Sender};
use iced::widget::{
    button, checkbox, column, container, pick_list, row, scrollable, text, text_input, Column,
};
use iced::{
    theme, window, Alignment, Background, Border, Color, Element, Length, Shadow, Size,
    Subscription, Task, Theme,
};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

const LOG_CAPACITY: usize = 500;

pub struct BacnetRepublisher {
    config: AppConfig,
    config_path: PathBuf,
    selected_page: Page,
    interfaces: Vec<NetworkInterface>,
    interface_choices: Vec<Ipv4Addr>,
    devices: Vec<DiscoveredDevice>,
    scanned_objects: Vec<DeviceObject>,
    samples: Vec<PointSample>,
    point_statuses: HashMap<PointIdentity, PointStatus>,
    status: String,
    settings: SettingsDraft,
    point_editor: PointEditor,
    selected_point: Option<usize>,
    worker_sender: Sender<WorkerEvent>,
    worker_receiver: Receiver<WorkerEvent>,
    logs: LogBuffer,
    working: bool,
    republishing: bool,
    republisher_stop: Option<Arc<AtomicBool>>,
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
    NewPoint,
    TogglePoint(usize, bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Discover,
    Points,
    Republish,
    Settings,
    Logs,
}

impl std::fmt::Display for Page {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
            .window(window::Settings {
                size: Size::new(1180.0, 760.0),
                min_size: Some(Size::new(920.0, 620.0)),
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
                selected_page: Page::Discover,
                interfaces,
                interface_choices,
                devices: Vec::new(),
                scanned_objects: Vec::new(),
                samples: Vec::new(),
                point_statuses: HashMap::new(),
                status,
                selected_point: None,
                worker_sender,
                worker_receiver,
                logs,
                working: false,
                republishing: false,
                republisher_stop: None,
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
        let palette = self.palette();
        theme::Style {
            background_color: palette.background,
            text_color: palette.text,
        }
    }

    fn subscription(&self) -> Subscription<Message> {
        iced::time::every(Duration::from_millis(250)).map(|_| Message::DrainWorkerEvents)
    }

    fn update(&mut self, message: Message) -> Task<Message> {
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
                self.save_config_with_status();
            }
            Message::Discover => self.start_discovery(),
            Message::ScanObjects(device_instance) => self.start_object_scan(device_instance),
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
                text("NETIX").size(16),
                text("BACnet Republisher").size(22),
                iced::widget::rule::horizontal(1),
                self.nav_button(Page::Discover),
                self.nav_button(Page::Points),
                self.nav_button(Page::Republish),
                self.nav_button(Page::Settings),
                self.nav_button(Page::Logs),
            ]
            .spacing(10)
            .padding(16)
            .width(Length::Fixed(230.0)),
        )
        .style(move |_| container_style(palette.panel, palette.border));

        let content = match self.selected_page {
            Page::Discover => self.discover_page(),
            Page::Points => self.points_page(),
            Page::Republish => self.republish_page(),
            Page::Settings => self.settings_page(),
            Page::Logs => self.logs_page(),
        };

        row![sidebar, content].height(Length::Fill).into()
    }

    fn discover_page(&self) -> Element<'_, Message> {
        let controls = row![
            button("Refresh NICs").on_press(Message::RefreshInterfaces),
            button("Discover").on_press(Message::Discover),
            checkbox(self.config.bacnet.discover_all_interfaces)
                .label("All interfaces")
                .on_toggle(Message::DiscoverAllInterfacesChanged),
            pick_list(
                self.interface_choices.clone(),
                self.config.bacnet.selected_interface,
                Message::InterfaceSelected
            )
            .placeholder("Bind interface"),
        ]
        .spacing(12)
        .align_y(Alignment::Center);

        let mut devices = column![section_title("Discovered devices")].spacing(8);
        if self.devices.is_empty() {
            devices = devices.push(text("No BACnet devices discovered yet."));
        } else {
            for device in &self.devices {
                devices = devices.push(
                    row![
                        text(format!("Device {}", device.instance)).width(Length::FillPortion(2)),
                        text(device.address.clone()).width(Length::FillPortion(2)),
                        text(format!("Vendor {}", device.vendor_id)).width(Length::FillPortion(1)),
                        button("Scan objects").on_press(Message::ScanObjects(device.instance)),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                );
            }
        }

        let mut objects = column![section_title("Scanned objects")].spacing(8);
        if self.scanned_objects.is_empty() {
            objects = objects.push(text("Scan a device object list or add points manually."));
        } else {
            for (index, object) in self.scanned_objects.iter().enumerate() {
                objects = objects.push(
                    row![
                        text(format!("Device {}", object.device_instance))
                            .width(Length::FillPortion(1)),
                        text(format!("{} {}", object.object_type, object.object_instance))
                            .width(Length::FillPortion(2)),
                        text(self.object_summary(object)).width(Length::FillPortion(3)),
                        button("Add point").on_press(Message::AddObjectAsPoint(index)),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                );
            }
        }

        self.page_shell(
            "Discover",
            "Find BACnet/IP devices and seed point configuration.",
            column![controls, card(devices), card(objects)].spacing(16),
        )
    }

    fn points_page(&self) -> Element<'_, Message> {
        let editor = card(
            column![
                section_title(if self.selected_point.is_some() {
                    "Edit point"
                } else {
                    "New point"
                }),
                row![
                    labeled_input(
                        "Device instance",
                        &self.point_editor.device_instance,
                        Message::PointDeviceInstanceChanged
                    ),
                    labeled_input(
                        "Device label",
                        &self.point_editor.device_label,
                        Message::PointDeviceLabelChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Object type",
                        &self.point_editor.object_type,
                        Message::PointObjectTypeChanged
                    ),
                    labeled_input(
                        "Object instance",
                        &self.point_editor.object_instance,
                        Message::PointObjectInstanceChanged
                    ),
                    labeled_input(
                        "Property",
                        &self.point_editor.property,
                        Message::PointPropertyChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Tag path",
                        &self.point_editor.tag_path,
                        Message::PointTagPathChanged
                    ),
                    labeled_input(
                        "Poll seconds",
                        &self.point_editor.poll_interval_secs,
                        Message::PointPollIntervalChanged
                    ),
                ]
                .spacing(12),
                row![
                    checkbox(self.point_editor.enabled)
                        .label("Enabled")
                        .on_toggle(Message::PointEnabledChanged),
                    button("Save point").on_press(Message::SavePoint),
                    button("New point").on_press(Message::NewPoint),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
            ]
            .spacing(12),
        );

        let mut list = column![section_title("Configured points")].spacing(8);
        if self.config.points.is_empty() {
            list = list.push(text("No points configured."));
        } else {
            for (index, point) in self.config.points.iter().enumerate() {
                list = list.push(
                    row![
                        checkbox(point.enabled)
                            .label("")
                            .on_toggle(move |value| Message::TogglePoint(index, value)),
                        text(format!("Device {}", point.device_instance))
                            .width(Length::FillPortion(1)),
                        text(point.display_name()).width(Length::FillPortion(2)),
                        text(if point.tag_path.is_empty() {
                            "(default topic)"
                        } else {
                            &point.tag_path
                        })
                        .width(Length::FillPortion(2)),
                        text(self.point_status_label(point)).width(Length::FillPortion(2)),
                        button("Edit").on_press(Message::EditPoint(index)),
                        button("Delete").on_press(Message::DeletePoint(index)),
                    ]
                    .spacing(10)
                    .align_y(Alignment::Center),
                );
            }
        }

        self.page_shell(
            "Points",
            "Manage BACnet objects and MQTT tag paths.",
            column![editor, card(list)].spacing(16),
        )
    }

    fn republish_page(&self) -> Element<'_, Message> {
        let mut samples = column![section_title("Last samples")].spacing(8);
        if self.samples.is_empty() {
            samples = samples.push(text("No point samples published yet."));
        } else {
            for sample in self.samples.iter().rev().take(20) {
                samples = samples.push(
                    row![
                        text(sample.topic.clone()).width(Length::FillPortion(3)),
                        text(sample.value.to_string()).width(Length::FillPortion(1)),
                        text(sample.timestamp_ms.to_string()).width(Length::FillPortion(1)),
                    ]
                    .spacing(12),
                );
            }
        }

        let summary = card(
            column![
                section_title("MQTT target"),
                text(format!(
                    "{}:{} ({})",
                    self.config.mqtt.host,
                    self.config.mqtt.port,
                    if self.config.mqtt.use_tls {
                        "TLS"
                    } else {
                        "plain TCP"
                    }
                )),
                text(format!("Topic prefix: {}", self.config.mqtt.topic_prefix)),
                text(format!("Health topic: {}", self.config.mqtt.health_topic)),
                row![
                    button("Poll once").on_press(Message::PollAndPublish),
                    button(if self.republishing {
                        "Republishing"
                    } else {
                        "Start"
                    })
                    .on_press(Message::StartRepublisher),
                    button("Stop").on_press(Message::StopRepublisher),
                    text(format!("{} enabled point(s)", self.enabled_point_count())),
                ]
                .spacing(12)
                .align_y(Alignment::Center),
            ]
            .spacing(10),
        );

        self.page_shell(
            "Republish",
            "Poll configured BACnet points and publish scalar MQTT values.",
            column![summary, card(samples)].spacing(16),
        )
    }

    fn settings_page(&self) -> Element<'_, Message> {
        let bacnet = card(
            column![
                section_title("BACnet/IP"),
                row![
                    labeled_input(
                        "Port",
                        &self.settings.bacnet_port,
                        Message::BacnetPortChanged
                    ),
                    labeled_input(
                        "Broadcast address",
                        &self.settings.broadcast_address,
                        Message::BroadcastAddressChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Discovery window ms",
                        &self.settings.discovery_window_ms,
                        Message::DiscoveryWindowChanged
                    ),
                    labeled_input(
                        "APDU timeout ms",
                        &self.settings.apdu_timeout_ms,
                        Message::ApduTimeoutChanged
                    ),
                ]
                .spacing(12),
                checkbox(self.settings.bbmd_enabled)
                    .label("Register as foreign device through BBMD")
                    .on_toggle(Message::BbmdEnabledChanged),
                row![
                    labeled_input(
                        "BBMD address",
                        &self.settings.bbmd_address,
                        Message::BbmdAddressChanged
                    ),
                    labeled_input(
                        "BBMD port",
                        &self.settings.bbmd_port,
                        Message::BbmdPortChanged
                    ),
                    labeled_input("BBMD TTL", &self.settings.bbmd_ttl, Message::BbmdTtlChanged),
                ]
                .spacing(12),
            ]
            .spacing(12),
        );

        let mqtt = card(
            column![
                section_title("MQTT"),
                row![
                    labeled_input("Host", &self.settings.mqtt_host, Message::MqttHostChanged),
                    labeled_input("Port", &self.settings.mqtt_port, Message::MqttPortChanged),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Client ID",
                        &self.settings.mqtt_client_id,
                        Message::MqttClientIdChanged
                    ),
                    labeled_input(
                        "Keep-alive seconds",
                        &self.settings.mqtt_keep_alive_secs,
                        Message::MqttKeepAliveChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Topic prefix",
                        &self.settings.mqtt_topic_prefix,
                        Message::MqttTopicPrefixChanged
                    ),
                    labeled_input(
                        "Health topic",
                        &self.settings.mqtt_health_topic,
                        Message::MqttHealthTopicChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Username",
                        &self.settings.mqtt_username,
                        Message::MqttUsernameChanged
                    ),
                    labeled_input(
                        "Password",
                        &self.settings.mqtt_password,
                        Message::MqttPasswordChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "CA certificate PEM",
                        &self.settings.mqtt_ca_cert_path,
                        Message::MqttCaCertPathChanged
                    ),
                    labeled_input(
                        "Client certificate PEM",
                        &self.settings.mqtt_client_cert_path,
                        Message::MqttClientCertPathChanged
                    ),
                ]
                .spacing(12),
                row![
                    labeled_input(
                        "Client key PEM",
                        &self.settings.mqtt_client_key_path,
                        Message::MqttClientKeyPathChanged
                    ),
                    labeled_input(
                        "Client key passphrase",
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

        let ui = card(
            row![
                text("Theme").width(Length::Fixed(120.0)),
                pick_list(
                    UiTheme::ALL.to_vec(),
                    Some(self.config.ui.theme),
                    Message::ThemeSelected
                ),
                button("Save settings").on_press(Message::SaveSettings),
            ]
            .spacing(12)
            .align_y(Alignment::Center),
        );

        self.page_shell(
            "Settings",
            "Configure BACnet transport, MQTT destination, and local preferences.",
            column![bacnet, mqtt, ui].spacing(16),
        )
    }

    fn logs_page(&self) -> Element<'_, Message> {
        let mut log_list = column![].spacing(6);
        for entry in self.logs.entries() {
            log_list = log_list.push(text(format!(
                "#{:04} +{:>5}s [{}] {}",
                entry.sequence,
                entry.elapsed.as_secs(),
                entry.level,
                entry.message
            )));
        }

        self.page_shell(
            "Logs",
            "Recent app, BACnet, and MQTT activity.",
            column![
                row![
                    button("Clear logs").on_press(Message::ClearLogs),
                    text(self.status.clone())
                ]
                .spacing(12)
                .align_y(Alignment::Center),
                card(log_list)
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
        let header = column![text(title).size(30), text(subtitle).size(15)].spacing(4);
        container(scrollable(column![header, body].spacing(18).padding(24)))
            .width(Length::Fill)
            .height(Length::Fill)
            .into()
    }

    fn nav_button(&self, page: Page) -> Element<'_, Message> {
        let label = if self.selected_page == page {
            format!("> {page}")
        } else {
            format!("  {page}")
        };
        button(text(label).width(Length::Fill))
            .width(Length::Fill)
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
            device_instance,
        );
    }

    fn start_poll_and_publish(&mut self) {
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
            self.config.mqtt.clone(),
            self.config.points.clone(),
        );
    }

    fn start_republisher(&mut self) {
        if self.republishing {
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
        self.republishing = true;
        spawn_republisher(
            self.worker_sender.clone(),
            self.config.bacnet.clone(),
            self.config.mqtt.clone(),
            self.config.points.clone(),
            stop,
        );
    }

    fn stop_republisher(&mut self) {
        if let Some(stop) = &self.republisher_stop {
            stop.store(true, Ordering::Relaxed);
            self.set_status(LogLevel::Info, "Stopping continuous republisher");
        } else {
            self.set_status(LogLevel::Warning, "Continuous republisher is not running");
        }
        self.republisher_stop = None;
        self.republishing = false;
    }

    fn add_object_as_point(&mut self, index: usize) {
        let Some(object) = self.scanned_objects.get(index) else {
            return;
        };
        let point = point_from_object(object);
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
                    for sample in &self.samples {
                        let status = self
                            .point_statuses
                            .entry(PointIdentity::from_point(&sample.point))
                            .or_default();
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
                WorkerEvent::Finished(message) => {
                    self.working = false;
                    if message.contains("republisher stopped") {
                        self.republishing = false;
                        self.republisher_stop = None;
                    }
                    self.set_status(LogLevel::Info, message);
                }
            }
        }
    }

    fn record_samples(&mut self, samples: Vec<PointSample>) {
        for sample in &samples {
            self.point_statuses
                .entry(PointIdentity::from_point(&sample.point))
                .or_default()
                .record_sample(sample);
        }
        self.samples = samples;
    }

    fn point_status_label(&self, point: &PointConfig) -> String {
        let Some(status) = self.point_statuses.get(&PointIdentity::from_point(point)) else {
            return "No sample".to_string();
        };
        if let Some(error) = &status.last_error {
            return format!("Stale: {error}");
        }
        if let Some(error) = &status.last_publish_error {
            return format!("Publish error: {error}");
        }
        match (&status.last_value, status.last_sample_ms) {
            (Some(value), Some(timestamp)) => format!("OK {value} @ {timestamp}"),
            (Some(value), None) => format!("OK {value}"),
            _ if status.stale => "Stale".to_string(),
            _ => "OK".to_string(),
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

    fn set_status(&mut self, level: LogLevel, message: impl Into<String>) {
        let message = message.into();
        self.status = message.clone();
        self.logs.push(level, message);
    }

    fn palette(&self) -> Palette {
        match self.config.ui.theme {
            UiTheme::Light => Palette {
                background: Color::from_rgb8(244, 247, 248),
                panel: Color::WHITE,
                card: Color::from_rgb8(252, 253, 253),
                text: Color::from_rgb8(27, 32, 35),
                border: Color::from_rgb8(214, 222, 226),
            },
            UiTheme::Auto | UiTheme::Dark => Palette {
                background: Color::from_rgb8(18, 22, 24),
                panel: Color::from_rgb8(26, 32, 35),
                card: Color::from_rgb8(31, 38, 41),
                text: Color::from_rgb8(232, 238, 241),
                border: Color::from_rgb8(58, 70, 75),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Palette {
    background: Color,
    panel: Color,
    card: Color,
    text: Color,
    border: Color,
}

fn section_title(label: &str) -> Element<'_, Message> {
    text(label).size(18).into()
}

fn labeled_input<'a>(
    label: &'a str,
    value: &'a str,
    on_input: impl Fn(String) -> Message + 'a,
) -> Element<'a, Message> {
    column![
        text(label).size(13),
        text_input(label, value)
            .on_input(on_input)
            .padding(8)
            .width(Length::Fill),
    ]
    .spacing(4)
    .width(Length::Fill)
    .into()
}

fn card<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(content)
        .padding(16)
        .width(Length::Fill)
        .style(|theme: &Theme| {
            let palette = match theme {
                Theme::Light => Palette {
                    background: Color::from_rgb8(244, 247, 248),
                    panel: Color::WHITE,
                    card: Color::from_rgb8(252, 253, 253),
                    text: Color::BLACK,
                    border: Color::from_rgb8(214, 222, 226),
                },
                _ => Palette {
                    background: Color::from_rgb8(18, 22, 24),
                    panel: Color::from_rgb8(26, 32, 35),
                    card: Color::from_rgb8(31, 38, 41),
                    text: Color::WHITE,
                    border: Color::from_rgb8(58, 70, 75),
                },
            };
            container_style(palette.card, palette.border)
        })
        .into()
}

fn container_style(background: Color, border: Color) -> iced::widget::container::Style {
    iced::widget::container::Style {
        text_color: None,
        background: Some(Background::Color(background)),
        border: Border {
            color: border,
            width: 1.0,
            radius: 8.0.into(),
        },
        shadow: Shadow::default(),
        snap: false,
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
