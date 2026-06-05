use crate::model::{PointConfig, PointIdentity};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeImportResult {
    pub points: Vec<PointConfig>,
    pub added: usize,
    pub updated: usize,
}

pub fn merge_imported_points(
    existing: &[PointConfig],
    imported: &[PointConfig],
) -> MergeImportResult {
    let mut points = existing.to_vec();
    let mut index_by_id = HashMap::<PointIdentity, usize>::new();
    for (index, point) in points.iter().enumerate() {
        index_by_id.insert(PointIdentity::from_point(point), index);
    }

    let mut added = 0;
    let mut updated = 0;
    for point in imported {
        let identity = PointIdentity::from_point(point);
        if let Some(&index) = index_by_id.get(&identity) {
            let existing_point = &mut points[index];
            let mut changed = false;
            if !point.tag_path.trim().is_empty() && existing_point.tag_path != point.tag_path {
                existing_point.tag_path = point.tag_path.clone();
                changed = true;
            }
            if !point.device_label.trim().is_empty()
                && existing_point.device_label != point.device_label
            {
                existing_point.device_label = point.device_label.clone();
                changed = true;
            }
            if changed {
                updated += 1;
            }
        } else {
            index_by_id.insert(identity, points.len());
            points.push(point.clone());
            added += 1;
        }
    }

    MergeImportResult {
        points,
        added,
        updated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_adds_new_points_and_updates_tag_paths() {
        let existing = vec![PointConfig {
            device_instance: 1001,
            object_type: "analog_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            tag_path: "old/path".to_string(),
            ..PointConfig::default()
        }];
        let imported = vec![
            PointConfig {
                device_instance: 1001,
                object_type: "analog_input".to_string(),
                object_instance: 1,
                property: "present_value".to_string(),
                tag_path: "AHU-1/SupplyAirTemp".to_string(),
                device_label: "AHU-1".to_string(),
                ..PointConfig::default()
            },
            PointConfig {
                device_instance: 1001,
                object_type: "binary_value".to_string(),
                object_instance: 2,
                property: "present_value".to_string(),
                tag_path: "AHU-1/FanStatus".to_string(),
                ..PointConfig::default()
            },
        ];

        let result = merge_imported_points(&existing, &imported);

        assert_eq!(result.added, 1);
        assert_eq!(result.updated, 1);
        assert_eq!(result.points.len(), 2);
        assert_eq!(result.points[0].tag_path, "AHU-1/SupplyAirTemp");
    }
}
