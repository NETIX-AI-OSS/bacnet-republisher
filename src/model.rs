use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::Ipv4Addr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkInterface {
    pub name: String,
    pub addr: Ipv4Addr,
}

impl fmt::Display for NetworkInterface {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.name, self.addr)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PointConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub device_instance: u32,
    #[serde(default)]
    pub device_label: String,
    #[serde(default = "default_object_type")]
    pub object_type: String,
    pub object_instance: u32,
    #[serde(default = "default_property")]
    pub property: String,
    #[serde(default)]
    pub tag_path: String,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
}

impl PointConfig {
    pub fn display_name(&self) -> String {
        format!(
            "{} {} {}",
            self.object_type, self.object_instance, self.property
        )
    }
}

impl Default for PointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            device_instance: 0,
            device_label: String::new(),
            object_type: default_object_type(),
            object_instance: 0,
            property: default_property(),
            tag_path: String::new(),
            poll_interval_secs: default_poll_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    pub instance: u32,
    pub address: String,
    pub vendor_id: u16,
    pub max_apdu_length: u32,
    pub last_seen_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceObject {
    pub device_instance: u32,
    pub object_type: String,
    pub object_instance: u32,
    pub object_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointSample {
    pub point: PointConfig,
    pub value: TelemetryValue,
    pub topic: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointFailure {
    pub point: PointConfig,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PollOutcome {
    pub samples: Vec<PointSample>,
    pub failures: Vec<PointFailure>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TelemetryValue {
    Number(f64),
    Text(String),
}

impl TelemetryValue {
    pub fn as_json_value(&self) -> serde_json::Value {
        match self {
            Self::Number(value) => serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Text(value) => serde_json::Value::String(value.clone()),
        }
    }
}

impl fmt::Display for TelemetryValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => write!(formatter, "{value:.3}"),
            Self::Text(value) => formatter.write_str(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishStats {
    pub published: usize,
    pub failed: usize,
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub fn default_true() -> bool {
    true
}

pub fn default_object_type() -> String {
    "analog_input".to_string()
}

pub fn default_property() -> String {
    "present_value".to_string()
}

pub fn default_poll_interval_secs() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_value_encodes_json_scalars() {
        assert_eq!(
            TelemetryValue::Number(12.5).as_json_value(),
            serde_json::json!(12.5)
        );
        assert_eq!(
            TelemetryValue::Text("active".to_string()).as_json_value(),
            serde_json::json!("active")
        );
    }
}
