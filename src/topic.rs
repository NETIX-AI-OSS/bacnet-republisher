use crate::config::MqttConfig;
use crate::model::PointConfig;

pub fn telemetry_topic(config: &MqttConfig, point: &PointConfig) -> String {
    let tag_path = if point.tag_path.trim().is_empty() {
        default_tag_path(point)
    } else {
        point.tag_path.clone()
    };
    join_topic(&[&normalize_prefix(&config.topic_prefix), &tag_path])
}

pub fn default_tag_path(point: &PointConfig) -> String {
    let device = if point.device_label.trim().is_empty() {
        format!("device_{}", point.device_instance)
    } else {
        point.device_label.clone()
    };
    format!(
        "{}/{}/{}/{}",
        sanitize_segment(&device),
        sanitize_segment(&point.object_type),
        point.object_instance,
        sanitize_segment(&point.property)
    )
}

pub fn normalize_prefix(prefix: &str) -> String {
    prefix
        .trim()
        .trim_end_matches('#')
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .map(sanitize_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn join_topic(parts: &[&str]) -> String {
    parts
        .iter()
        .flat_map(|part| part.split('/'))
        .filter(|segment| !segment.trim().is_empty())
        .map(sanitize_segment)
        .collect::<Vec<_>>()
        .join("/")
}

pub fn sanitize_segment(value: &str) -> String {
    let mut sanitized = value
        .trim()
        .chars()
        .map(|character| match character {
            '/' | '#' | '+' | ' ' | '\t' | '\n' | '\r' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect::<String>();

    while sanitized.contains("__") {
        sanitized = sanitized.replace("__", "_");
    }
    sanitized.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MqttConfig;

    #[test]
    fn normalizes_abstract_subscription_prefix() {
        assert_eq!(normalize_prefix("Netix/NC-9/#"), "Netix/NC-9");
        assert_eq!(normalize_prefix("/Netix//Site/"), "Netix/Site");
    }

    #[test]
    fn generates_topic_from_default_point_path() {
        let config = MqttConfig {
            topic_prefix: "Netix/NC-9/#".to_string(),
            ..MqttConfig::default()
        };
        let point = PointConfig {
            device_instance: 100,
            device_label: "Jace Neo".to_string(),
            object_type: "analog input".to_string(),
            object_instance: 2,
            property: "present_value".to_string(),
            ..PointConfig::default()
        };

        assert_eq!(
            telemetry_topic(&config, &point),
            "Netix/NC-9/Jace_Neo/analog_input/2/present_value"
        );
    }

    #[test]
    fn custom_tag_path_wins() {
        let config = MqttConfig::default();
        let point = PointConfig {
            tag_path: "AHU1/Supply Temp".to_string(),
            ..PointConfig::default()
        };

        assert_eq!(
            telemetry_topic(&config, &point),
            "Netix/Site/AHU1/Supply_Temp"
        );
    }
}
