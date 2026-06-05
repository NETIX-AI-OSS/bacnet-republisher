use crate::model::PointConfig;
use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

const CONFIG_FILE_NAME: &str = "config.toml";
pub const CURRENT_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub version: u32,
    pub bacnet: BacnetConfig,
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub points: Vec<PointConfig>,
    pub ui: UiPreferences,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BacnetConfig {
    #[serde(default)]
    pub selected_interface: Option<Ipv4Addr>,
    #[serde(default = "default_true")]
    pub discover_all_interfaces: bool,
    #[serde(default = "default_bacnet_port")]
    pub port: u16,
    #[serde(default = "default_broadcast_address")]
    pub broadcast_address: Ipv4Addr,
    #[serde(default = "default_discovery_window_ms")]
    pub discovery_window_ms: u64,
    #[serde(default = "default_apdu_timeout_ms")]
    pub apdu_timeout_ms: u64,
    #[serde(default)]
    pub bbmd: Option<BbmdConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BbmdConfig {
    pub address: Ipv4Addr,
    #[serde(default = "default_bacnet_port")]
    pub port: u16,
    #[serde(default = "default_foreign_device_ttl_secs")]
    pub ttl_secs: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MqttConfig {
    #[serde(default = "default_mqtt_host")]
    pub host: String,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    #[serde(default = "default_true")]
    pub use_tls: bool,
    #[serde(default = "default_client_id")]
    pub client_id: String,
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_health_topic")]
    pub health_topic: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub remember_secrets: bool,
    #[serde(default)]
    pub retain: bool,
    #[serde(default = "default_keep_alive_secs")]
    pub keep_alive_secs: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UiTheme {
    Auto,
    Light,
    Dark,
}

impl std::fmt::Display for UiTheme {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => formatter.write_str("Auto"),
            Self::Light => formatter.write_str("Light"),
            Self::Dark => formatter.write_str("Dark"),
        }
    }
}

impl UiTheme {
    pub const ALL: [Self; 3] = [Self::Auto, Self::Light, Self::Dark];
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UiPreferences {
    #[serde(default = "default_ui_theme")]
    pub theme: UiTheme,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: CURRENT_CONFIG_VERSION,
            bacnet: BacnetConfig::default(),
            mqtt: MqttConfig::default(),
            points: Vec::new(),
            ui: UiPreferences::default(),
        }
    }
}

impl AppConfig {
    pub fn migrate(&mut self) {
        self.version = CURRENT_CONFIG_VERSION;
    }

    pub fn sanitized_for_save(&self) -> Self {
        let mut clone = self.clone();
        if !clone.mqtt.remember_secrets {
            clone.mqtt.password = None;
        }
        clone
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.bacnet.port == 0 {
            return Err("BACnet port cannot be 0 for discovery".to_string());
        }
        if self.bacnet.discovery_window_ms < 250 {
            return Err("BACnet discovery window must be at least 250 ms".to_string());
        }
        if self.bacnet.apdu_timeout_ms < 250 {
            return Err("BACnet APDU timeout must be at least 250 ms".to_string());
        }
        if self.mqtt.host.trim().is_empty() {
            return Err("MQTT host cannot be empty".to_string());
        }
        if self.mqtt.port == 0 {
            return Err("MQTT port cannot be 0".to_string());
        }
        if self.mqtt.topic_prefix.trim().is_empty() {
            return Err("MQTT topic prefix cannot be empty".to_string());
        }
        for point in &self.points {
            if point.enabled && point.device_instance == 0 {
                return Err(format!(
                    "{} has no BACnet device instance",
                    point.display_name()
                ));
            }
            if point.enabled && point.poll_interval_secs == 0 {
                return Err(format!(
                    "{} poll interval cannot be 0",
                    point.display_name()
                ));
            }
        }
        Ok(())
    }
}

impl Default for BacnetConfig {
    fn default() -> Self {
        Self {
            selected_interface: None,
            discover_all_interfaces: true,
            port: default_bacnet_port(),
            broadcast_address: default_broadcast_address(),
            discovery_window_ms: default_discovery_window_ms(),
            apdu_timeout_ms: default_apdu_timeout_ms(),
            bbmd: None,
        }
    }
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: default_mqtt_host(),
            port: default_mqtt_port(),
            use_tls: true,
            client_id: default_client_id(),
            topic_prefix: default_topic_prefix(),
            health_topic: default_health_topic(),
            username: None,
            password: None,
            remember_secrets: false,
            retain: false,
            keep_alive_secs: default_keep_alive_secs(),
        }
    }
}

impl Default for UiPreferences {
    fn default() -> Self {
        Self {
            theme: default_ui_theme(),
        }
    }
}

pub fn config_path() -> Result<PathBuf> {
    let project_dirs = ProjectDirs::from("com", "netix", "bacnet-republisher")
        .context("failed to resolve OS config directory")?;
    Ok(project_dirs.config_dir().join(CONFIG_FILE_NAME))
}

pub fn load_or_default() -> (AppConfig, PathBuf, String) {
    let path = match config_path() {
        Ok(path) => path,
        Err(error) => {
            return (
                AppConfig::default(),
                PathBuf::from(CONFIG_FILE_NAME),
                error.to_string(),
            )
        }
    };

    match load_from_path(&path) {
        Ok(config) => (config, path, "Loaded saved configuration".to_string()),
        Err(error) if path.exists() => (
            AppConfig::default(),
            path,
            format!("Using defaults; config load failed: {error:#}"),
        ),
        Err(_) => (
            AppConfig::default(),
            path,
            "Using default configuration".to_string(),
        ),
    }
}

pub fn load_from_path(path: &Path) -> Result<AppConfig> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut config: AppConfig =
        toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
    config.migrate();
    Ok(config)
}

pub fn save_to_path(path: &Path, config: &AppConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let raw =
        toml::to_string_pretty(&config.sanitized_for_save()).context("failed to encode config")?;
    fs::write(path, raw).with_context(|| format!("failed to write {}", path.display()))
}

fn default_true() -> bool {
    true
}

fn default_bacnet_port() -> u16 {
    0xBAC0
}

fn default_broadcast_address() -> Ipv4Addr {
    Ipv4Addr::BROADCAST
}

fn default_discovery_window_ms() -> u64 {
    3_000
}

fn default_apdu_timeout_ms() -> u64 {
    2_000
}

fn default_foreign_device_ttl_secs() -> u16 {
    300
}

fn default_mqtt_host() -> String {
    "localhost".to_string()
}

fn default_mqtt_port() -> u16 {
    8883
}

fn default_client_id() -> String {
    "netix-bacnet-republisher".to_string()
}

fn default_topic_prefix() -> String {
    "Netix/Site".to_string()
}

fn default_health_topic() -> String {
    "Netix/Site/_health/bacnet-republisher".to_string()
}

fn default_keep_alive_secs() -> u64 {
    30
}

fn default_ui_theme() -> UiTheme {
    UiTheme::Auto
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_omits_password_when_secret_persistence_is_disabled() {
        let mut config = AppConfig::default();
        config.mqtt.username = Some("user".to_string());
        config.mqtt.password = Some("secret".to_string());
        config.mqtt.remember_secrets = false;

        let saved = config.sanitized_for_save();

        assert_eq!(saved.mqtt.username.as_deref(), Some("user"));
        assert_eq!(saved.mqtt.password, None);
    }

    #[test]
    fn save_keeps_password_when_secret_persistence_is_enabled() {
        let mut config = AppConfig::default();
        config.mqtt.password = Some("secret".to_string());
        config.mqtt.remember_secrets = true;

        let saved = config.sanitized_for_save();

        assert_eq!(saved.mqtt.password.as_deref(), Some("secret"));
    }

    #[test]
    fn config_round_trips_toml() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config.toml");
        let mut config = AppConfig::default();
        config.points.push(PointConfig {
            device_instance: 1234,
            object_instance: 1,
            tag_path: "AHU1/SupplyTemp".to_string(),
            ..PointConfig::default()
        });

        save_to_path(&path, &config).unwrap();
        let loaded = load_from_path(&path).unwrap();

        assert_eq!(loaded.points.len(), 1);
        assert_eq!(loaded.points[0].tag_path, "AHU1/SupplyTemp");
    }
}
