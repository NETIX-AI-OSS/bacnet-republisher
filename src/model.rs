use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
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

#[derive(Debug, Clone, PartialEq)]
pub struct DeviceObject {
    pub device_instance: u32,
    pub object_type: String,
    pub object_instance: u32,
    pub object_name: Option<String>,
    pub description: Option<String>,
    pub units: Option<String>,
    pub present_value: Option<TelemetryValue>,
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
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverOutcome {
    pub devices: Vec<DiscoveredDevice>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BulkTagImportOutcome {
    pub devices: Vec<DiscoveredDevice>,
    pub scanned_objects: Vec<DeviceObject>,
    pub points: Vec<PointConfig>,
    pub added: usize,
    pub updated: usize,
    pub warnings: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishStats {
    pub queued: usize,
    pub published: usize,
    pub failed: usize,
    pub reconnects: usize,
    pub last_error: Option<String>,
}

impl PublishStats {
    pub fn empty() -> Self {
        Self {
            queued: 0,
            published: 0,
            failed: 0,
            reconnects: 0,
            last_error: None,
        }
    }

    pub fn record_failure(&mut self, error: impl Into<String>) {
        self.failed += 1;
        self.last_error = Some(error.into());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointIdentity {
    pub device_instance: u32,
    pub object_type: String,
    pub object_instance: u32,
    pub property: String,
}

impl PointIdentity {
    pub fn from_point(point: &PointConfig) -> Self {
        Self {
            device_instance: point.device_instance,
            object_type: normalize_identity_part(&point.object_type),
            object_instance: point.object_instance,
            property: normalize_identity_part(&point.property),
        }
    }
}

impl Hash for PointIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.device_instance.hash(state);
        self.object_type.hash(state);
        self.object_instance.hash(state);
        self.property.hash(state);
    }
}

impl fmt::Display for PointIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}:{}",
            self.device_instance, self.object_type, self.object_instance, self.property
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointStatus {
    pub last_value: Option<TelemetryValue>,
    pub last_sample_ms: Option<i64>,
    pub stale: bool,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub last_publish_error: Option<String>,
}

impl Default for PointStatus {
    fn default() -> Self {
        Self {
            last_value: None,
            last_sample_ms: None,
            stale: true,
            consecutive_failures: 0,
            last_error: None,
            last_publish_error: None,
        }
    }
}

impl PointStatus {
    pub fn record_sample(&mut self, sample: &PointSample) {
        self.last_value = Some(sample.value.clone());
        self.last_sample_ms = Some(sample.timestamp_ms);
        self.stale = false;
        self.consecutive_failures = 0;
        self.last_error = None;
    }

    pub fn record_read_failure(&mut self, error: impl Into<String>) {
        self.stale = true;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_error = Some(error.into());
    }

    pub fn record_publish_success(&mut self) {
        self.last_publish_error = None;
    }

    pub fn record_publish_failure(&mut self, error: impl Into<String>) {
        self.last_publish_error = Some(error.into());
    }
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

fn normalize_identity_part(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace([' ', '-'], "_")
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

    #[test]
    fn point_status_success_clears_stale_state() {
        let point = PointConfig {
            device_instance: 100,
            object_instance: 1,
            ..PointConfig::default()
        };
        let sample = PointSample {
            point,
            value: TelemetryValue::Number(10.0),
            topic: "Netix/Site/device_100/analog_input_1/present_value".to_string(),
            timestamp_ms: 42,
        };
        let mut status = PointStatus::default();
        status.record_read_failure("timeout");

        status.record_sample(&sample);

        assert!(!status.stale);
        assert_eq!(status.consecutive_failures, 0);
        assert_eq!(status.last_error, None);
        assert_eq!(status.last_value, Some(TelemetryValue::Number(10.0)));
    }

    #[test]
    fn point_status_failure_marks_stale() {
        let mut status = PointStatus::default();

        status.record_read_failure("timeout");
        status.record_read_failure("timeout again");

        assert!(status.stale);
        assert_eq!(status.consecutive_failures, 2);
        assert_eq!(status.last_error.as_deref(), Some("timeout again"));
    }
}
