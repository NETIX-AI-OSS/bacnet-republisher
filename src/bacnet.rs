use crate::config::{BacnetConfig, MqttConfig};
use crate::model::{
    DeviceObject, DiscoveredDevice, NetworkInterface, PointConfig, PointFailure, PointSample,
    PollOutcome,
};
use crate::topic::telemetry_topic;
use crate::value::{
    decode_object_identifier_value, decode_scalar_value, decode_unsigned_value,
    object_type_from_text, object_type_name, property_identifier_from_text,
};
use anyhow::{anyhow, Context, Result};
use bacnet_client::client::BACnetClient;
use bacnet_services::common::PropertyReference;
use bacnet_services::rpm::ReadAccessSpecification;
use bacnet_transport::bip::{BipTransport, ForeignDeviceConfig};
use bacnet_types::enums::{ObjectType, PropertyIdentifier};
use bacnet_types::primitives::ObjectIdentifier;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::time::Duration;

pub type BacnetIpClient = BACnetClient<BipTransport>;

pub async fn discover_devices(
    config: &BacnetConfig,
    interfaces: &[NetworkInterface],
) -> Result<Vec<DiscoveredDevice>> {
    let mut by_instance = HashMap::<u32, DiscoveredDevice>::new();

    for interface in target_interfaces(config, interfaces) {
        let mut client = build_client(config, interface)
            .await
            .with_context(|| format!("failed to start BACnet client on {interface}"))?;
        client.who_is(None, None).await?;
        tokio::time::sleep(Duration::from_millis(config.discovery_window_ms)).await;

        for device in collect_discovered_devices(&client).await {
            by_instance.insert(device.instance, device);
        }
        client.stop().await?;
    }

    let mut devices = by_instance.into_values().collect::<Vec<_>>();
    devices.sort_by_key(|device| device.instance);
    Ok(devices)
}

pub async fn scan_device_objects(
    config: &BacnetConfig,
    device_instance: u32,
    max_objects: usize,
) -> Result<Vec<DeviceObject>> {
    let mut client = build_client(
        config,
        config.selected_interface.unwrap_or(Ipv4Addr::UNSPECIFIED),
    )
    .await
    .context("failed to start BACnet client")?;
    client
        .who_is(Some(device_instance), Some(device_instance))
        .await?;
    tokio::time::sleep(Duration::from_millis(config.discovery_window_ms)).await;

    let objects = scan_device_objects_with_client(&client, device_instance, max_objects).await;
    client.stop().await?;
    objects
}

pub async fn collect_discovered_devices(client: &BacnetIpClient) -> Vec<DiscoveredDevice> {
    let mut devices = client
        .discovered_devices()
        .await
        .into_iter()
        .map(|device| {
            let instance = device.object_identifier.instance_number();
            DiscoveredDevice {
                instance,
                address: format_bip_mac(device.mac_address.as_slice()),
                vendor_id: device.vendor_id,
                max_apdu_length: device.max_apdu_length,
                last_seen_ms: device.last_seen.elapsed().as_millis(),
            }
        })
        .collect::<Vec<_>>();
    devices.sort_by_key(|device| device.instance);
    devices
}

pub async fn scan_device_objects_with_client(
    client: &BacnetIpClient,
    device_instance: u32,
    max_objects: usize,
) -> Result<Vec<DeviceObject>> {
    let device_oid = ObjectIdentifier::new(ObjectType::DEVICE, device_instance)?;
    let count_ack = client
        .read_property_from_device(
            device_instance,
            device_oid,
            PropertyIdentifier::OBJECT_LIST,
            Some(0),
        )
        .await
        .context("failed to read objectList array length")?;
    let count = decode_unsigned_value(&count_ack.property_value)
        .context("failed to decode objectList length")? as usize;

    let mut objects = Vec::new();
    for index in 1..=count.min(max_objects) {
        let ack = client
            .read_property_from_device(
                device_instance,
                device_oid,
                PropertyIdentifier::OBJECT_LIST,
                Some(index as u32),
            )
            .await;
        let Ok(ack) = ack else {
            continue;
        };
        let Ok((object_type, object_instance)) =
            decode_object_identifier_value(&ack.property_value)
        else {
            continue;
        };
        if object_type == ObjectType::DEVICE {
            continue;
        }
        let object_identifier = ObjectIdentifier::new(object_type, object_instance)?;
        let object_name = read_scalar_property(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::OBJECT_NAME,
        )
        .await
        .map(|value| value.to_string())
        .filter(|value| !value.trim().is_empty());
        let description = read_scalar_property(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::DESCRIPTION,
        )
        .await
        .map(|value| value.to_string())
        .filter(|value| !value.trim().is_empty());
        let units = read_scalar_property(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::UNITS,
        )
        .await
        .map(|value| value.to_string())
        .filter(|value| !value.trim().is_empty());
        let present_value = read_scalar_property(
            client,
            device_instance,
            object_identifier,
            PropertyIdentifier::PRESENT_VALUE,
        )
        .await;

        objects.push(DeviceObject {
            device_instance,
            object_type: object_type_name(object_type),
            object_instance,
            object_name,
            description,
            units,
            present_value,
        });
    }

    Ok(objects)
}

pub async fn poll_points_once(
    bacnet: &BacnetConfig,
    mqtt: &MqttConfig,
    points: &[PointConfig],
) -> Result<PollOutcome> {
    let enabled = points
        .iter()
        .filter(|point| point.enabled)
        .cloned()
        .collect::<Vec<_>>();
    if enabled.is_empty() {
        return Ok(PollOutcome {
            samples: Vec::new(),
            failures: Vec::new(),
            warnings: Vec::new(),
        });
    }

    let mut client = build_client(
        bacnet,
        bacnet.selected_interface.unwrap_or(Ipv4Addr::UNSPECIFIED),
    )
    .await
    .context("failed to start BACnet client")?;
    client.who_is(None, None).await?;
    tokio::time::sleep(Duration::from_millis(bacnet.discovery_window_ms)).await;

    let outcome = poll_points_once_with_client(&client, mqtt, &enabled).await;
    client.stop().await?;
    outcome
}

pub async fn poll_points_once_with_client(
    client: &BacnetIpClient,
    mqtt: &MqttConfig,
    points: &[PointConfig],
) -> Result<PollOutcome> {
    let mut by_device = HashMap::<u32, Vec<PollRequest>>::new();
    let mut failures = Vec::new();
    for point in points.iter().filter(|point| point.enabled).cloned() {
        match PollRequest::from_point(point.clone()) {
            Ok(request) => by_device
                .entry(point.device_instance)
                .or_default()
                .push(request),
            Err(error) => failures.push(PointFailure {
                point,
                error: error.to_string(),
            }),
        }
    }

    let mut samples = Vec::new();
    let mut warnings = Vec::new();
    for (device_instance, requests) in by_device {
        match read_device_group_rpm(client, mqtt, device_instance, &requests).await {
            Ok(mut group_samples) => samples.append(&mut group_samples),
            Err(error) => {
                warnings.push(format!(
                    "RPM failed for device {device_instance}; used fallback: {error:#}"
                ));
                let fallback = read_device_group_individual(client, mqtt, &requests).await;
                for result in fallback {
                    match result {
                        Ok(sample) => samples.push(sample),
                        Err(failure) => failures.push(failure),
                    }
                }
            }
        }
    }

    Ok(PollOutcome {
        samples,
        failures,
        warnings,
    })
}

pub fn point_from_object(object: &DeviceObject) -> PointConfig {
    let label = format!("device_{}", object.device_instance);
    let path_name = object
        .object_name
        .as_ref()
        .filter(|name| !name.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| format!("{}_{}", object.object_type, object.object_instance));
    PointConfig {
        enabled: true,
        device_instance: object.device_instance,
        device_label: label,
        object_type: object.object_type.clone(),
        object_instance: object.object_instance,
        property: "present_value".to_string(),
        tag_path: path_name,
        poll_interval_secs: 10,
    }
}

struct PollRequest {
    point: PointConfig,
    object_identifier: ObjectIdentifier,
    property_identifier: PropertyIdentifier,
}

impl PollRequest {
    fn from_point(point: PointConfig) -> Result<Self> {
        let object_type = object_type_from_text(&point.object_type)
            .ok_or_else(|| anyhow!("unknown BACnet object type '{}'", point.object_type))?;
        let property_identifier = property_identifier_from_text(&point.property)
            .ok_or_else(|| anyhow!("unknown BACnet property '{}'", point.property))?;
        let object_identifier = ObjectIdentifier::new(object_type, point.object_instance)
            .context("invalid object identifier")?;
        Ok(Self {
            point,
            object_identifier,
            property_identifier,
        })
    }
}

async fn read_device_group_rpm(
    client: &BacnetIpClient,
    mqtt: &MqttConfig,
    device_instance: u32,
    requests: &[PollRequest],
) -> Result<Vec<PointSample>> {
    let specs = requests
        .iter()
        .map(|request| ReadAccessSpecification {
            object_identifier: request.object_identifier,
            list_of_property_references: vec![PropertyReference {
                property_identifier: request.property_identifier,
                property_array_index: None,
            }],
        })
        .collect::<Vec<_>>();

    let ack = client
        .read_property_multiple_from_device(device_instance, specs)
        .await?;
    let mut samples = Vec::new();
    let mut seen = HashSet::<usize>::new();

    for result in ack.list_of_read_access_results {
        for element in result.list_of_results {
            let Some((index, request)) = requests.iter().enumerate().find(|(_, request)| {
                request.object_identifier == result.object_identifier
                    && request.property_identifier == element.property_identifier
            }) else {
                continue;
            };
            let Some(value_bytes) = element.property_value else {
                continue;
            };
            let value = decode_scalar_value(&value_bytes)
                .with_context(|| format!("failed to decode {}", request.point.display_name()))?;
            seen.insert(index);
            samples.push(PointSample {
                point: request.point.clone(),
                topic: telemetry_topic(mqtt, &request.point),
                value,
                timestamp_ms: crate::model::now_millis(),
            });
        }
    }

    if seen.len() != requests.len() {
        return Err(anyhow!(
            "RPM returned {} of {} requested properties",
            seen.len(),
            requests.len()
        ));
    }

    Ok(samples)
}

async fn read_scalar_property(
    client: &BacnetIpClient,
    device_instance: u32,
    object_identifier: ObjectIdentifier,
    property_identifier: PropertyIdentifier,
) -> Option<crate::model::TelemetryValue> {
    client
        .read_property_from_device(
            device_instance,
            object_identifier,
            property_identifier,
            None,
        )
        .await
        .ok()
        .and_then(|ack| decode_scalar_value(&ack.property_value).ok())
}

async fn read_device_group_individual(
    client: &BacnetIpClient,
    mqtt: &MqttConfig,
    requests: &[PollRequest],
) -> Vec<Result<PointSample, PointFailure>> {
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let result = client
            .read_property_from_device(
                request.point.device_instance,
                request.object_identifier,
                request.property_identifier,
                None,
            )
            .await
            .map_err(|error| error.to_string())
            .and_then(|ack| {
                decode_scalar_value(&ack.property_value).map_err(|error| error.to_string())
            });

        results.push(match result {
            Ok(value) => Ok(PointSample {
                point: request.point.clone(),
                topic: telemetry_topic(mqtt, &request.point),
                value,
                timestamp_ms: crate::model::now_millis(),
            }),
            Err(error) => Err(PointFailure {
                point: request.point.clone(),
                error,
            }),
        });
    }
    results
}

async fn build_client(config: &BacnetConfig, interface: Ipv4Addr) -> Result<BacnetIpClient> {
    let mut transport = BipTransport::new(interface, config.port, config.broadcast_address);
    if let Some(bbmd) = &config.bbmd {
        transport.register_as_foreign_device(ForeignDeviceConfig {
            bbmd_ip: bbmd.address,
            bbmd_port: bbmd.port,
            ttl: bbmd.ttl_secs,
        });
    }

    BACnetClient::<BipTransport>::generic_builder()
        .transport(transport)
        .apdu_timeout_ms(config.apdu_timeout_ms)
        .build()
        .await
        .map_err(|error| anyhow!(error.to_string()))
}

fn target_interfaces(config: &BacnetConfig, interfaces: &[NetworkInterface]) -> Vec<Ipv4Addr> {
    if config.discover_all_interfaces {
        let mut addresses = interfaces
            .iter()
            .map(|interface| interface.addr)
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            addresses.push(Ipv4Addr::UNSPECIFIED);
        }
        addresses.sort();
        addresses.dedup();
        addresses
    } else {
        vec![config.selected_interface.unwrap_or(Ipv4Addr::UNSPECIFIED)]
    }
}

fn format_bip_mac(mac: &[u8]) -> String {
    if mac.len() >= 6 {
        let ip = Ipv4Addr::new(mac[0], mac[1], mac[2], mac[3]);
        let port = u16::from_be_bytes([mac[4], mac[5]]);
        format!("{ip}:{port}")
    } else {
        mac.iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BacnetConfig;

    #[test]
    fn point_from_object_uses_object_name_as_tag() {
        let object = DeviceObject {
            device_instance: 100,
            object_type: "analog_input".to_string(),
            object_instance: 2,
            object_name: Some("AHU1 Supply Temp".to_string()),
            description: None,
            units: None,
            present_value: None,
        };

        let point = point_from_object(&object);

        assert_eq!(point.device_instance, 100);
        assert_eq!(point.tag_path, "AHU1 Supply Temp");
    }

    #[test]
    fn target_interfaces_defaults_to_unspecified_without_interfaces() {
        let config = BacnetConfig::default();
        assert_eq!(target_interfaces(&config, &[]), vec![Ipv4Addr::UNSPECIFIED]);
    }

    #[test]
    fn parses_poll_request_identifiers() {
        let point = PointConfig {
            device_instance: 100,
            object_type: "analog_input".to_string(),
            object_instance: 1,
            property: "present_value".to_string(),
            ..PointConfig::default()
        };

        let request = PollRequest::from_point(point).unwrap();

        assert_eq!(
            request.object_identifier.object_type(),
            ObjectType::ANALOG_INPUT
        );
        assert_eq!(
            request.property_identifier,
            PropertyIdentifier::PRESENT_VALUE
        );
    }

    #[test]
    fn property_name_is_stable() {
        assert_eq!(
            crate::value::property_name(PropertyIdentifier::PRESENT_VALUE),
            "present_value"
        );
    }
}
