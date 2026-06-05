//! Default BACnet point definitions aligned with `bacnet-simulator/config.yaml`.
//!
//! Device IDs follow the simulator's `id_policy` (block_index + 1) * 100 + base 10000):
//!   - PLANT-MTR-001 = 10600 (template block 6: plant_meter)
//!   - AHU-L-001     = 10700 (template block 7: ahu_large)
//!   - VAV-OFC-001   = 10900 (template block 9: vav_office)
use crate::model::PointConfig;

pub fn simulator_points() -> Vec<PointConfig> {
    vec![
        PointConfig {
            enabled: true,
            device_instance: 10700,
            device_label: "AHU-L-001".to_string(),
            object_type: "analog_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "AHU-L-001/SupplyAirTemp".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10700,
            device_label: "AHU-L-001".to_string(),
            object_type: "analog_input".to_string(),
            object_instance: 2,
            property: "present_value".to_string(),
            tag_path: "AHU-L-001/ReturnAirTemp".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10700,
            device_label: "AHU-L-001".to_string(),
            object_type: "binary_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "AHU-L-001/SupplyFanStatus".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10900,
            device_label: "VAV-OFC-001".to_string(),
            object_type: "analog_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "VAV-OFC-001/RoomTemp".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10900,
            device_label: "VAV-OFC-001".to_string(),
            object_type: "analog_output".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "VAV-OFC-001/DamperPosition".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10600,
            device_label: "PLANT-MTR-001".to_string(),
            object_type: "analog_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "PLANT-MTR-001/ActivePower".to_string(),
            poll_interval_secs: 10,
        },
        PointConfig {
            enabled: true,
            device_instance: 10600,
            device_label: "PLANT-MTR-001".to_string(),
            object_type: "analog_input".to_string(),
            object_instance: 9,
            property: "present_value".to_string(),
            tag_path: "PLANT-MTR-001/TotalEnergy".to_string(),
            poll_interval_secs: 10,
        },
    ]
}
