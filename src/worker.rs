use crate::bacnet::{
    build_client, discover_devices, point_from_object, poll_points_once,
    poll_points_once_with_client, read_device_label, refresh_device_table, scan_device_objects,
    scan_device_objects_with_client,
};
use crate::config::{BacnetConfig, MqttConfig};
use crate::import::merge_imported_points;
use crate::log::LogLevel;
use crate::model::{
    BulkTagImportOutcome, DeviceObject, DiscoveredDevice, NetworkInterface, PointConfig,
    PointFailure, PointIdentity, PointSample, PointStatus, PollOutcome, PublishStats,
};
use crate::mqtt::{publish_health, HealthSnapshot, RumqttPublisher};
use crate::network::resolve_bacnet_bind_address;
use crossbeam_channel::Sender;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum WorkerEvent {
    Log(LogLevel, String),
    Devices(Vec<DiscoveredDevice>),
    Objects(Vec<DeviceObject>),
    ScanProgress { current: usize, total: usize },
    BulkTagImport(BulkTagImportOutcome),
    Samples(Vec<PointSample>),
    Failures(Vec<PointFailure>),
    PublishStatus(PublishStats),
    Finished(String),
}

pub fn spawn_discovery(
    sender: Sender<WorkerEvent>,
    config: BacnetConfig,
    interfaces: Vec<NetworkInterface>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    "Starting BACnet discovery".to_string(),
                ))
                .ok();
            match discover_devices(&config, &interfaces).await {
                Ok(outcome) => {
                    for warning in &outcome.warnings {
                        sender
                            .send(WorkerEvent::Log(LogLevel::Warning, warning.clone()))
                            .ok();
                    }
                    let count = outcome.devices.len();
                    sender.send(WorkerEvent::Devices(outcome.devices)).ok();
                    let summary = if outcome.warnings.is_empty() {
                        format!("Discovered {count} BACnet device(s)")
                    } else {
                        format!(
                            "Discovered {count} BACnet device(s) with {} interface warning(s)",
                            outcome.warnings.len()
                        )
                    };
                    sender.send(WorkerEvent::Finished(summary)).ok();
                }
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("Discovery failed: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished("Discovery failed".to_string()))
                        .ok();
                }
            }
        });
    });
}

const SCAN_ALL_MAX_OBJECTS_PER_DEVICE: usize = 512;

pub fn spawn_scan_all_objects(
    sender: Sender<WorkerEvent>,
    config: BacnetConfig,
    interfaces: Vec<NetworkInterface>,
    devices: Vec<DiscoveredDevice>,
    existing_points: Vec<PointConfig>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            let device_instances = devices.iter().map(|d| d.instance).collect::<Vec<_>>();
            let total = device_instances.len();
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    format!("Scanning object lists for {total} device(s)"),
                ))
                .ok();
            sender
                .send(WorkerEvent::ScanProgress { current: 0, total })
                .ok();

            // One shared client for the whole sweep. Building a fresh client per device
            // re-broadcasts Who-Is and races against the simulator's I-Am responses, which
            // is why per-device scans fail under load.
            let bind = resolve_bacnet_bind_address(&config, &interfaces);
            let mut client = match build_client(&config, bind).await {
                Ok(c) => c,
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("Failed to start BACnet client for scan-all: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished("Scan all failed".to_string()))
                        .ok();
                    return;
                }
            };

            match refresh_device_table(&client, &config, &device_instances).await {
                Ok(refresh) if !refresh.unresolved.is_empty() => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Warning,
                            format!(
                                "{} of {} device(s) not in I-Am cache after broadcast; their scans will fail",
                                refresh.unresolved.len(),
                                device_instances.len()
                            ),
                        ))
                        .ok();
                }
                Ok(_) => {}
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Warning,
                            format!("Device table refresh failed: {error:#}"),
                        ))
                        .ok();
                }
            }

            let mut all_objects = Vec::new();
            let mut imported_points = Vec::new();
            let mut failures = 0usize;
            for (idx, &device_instance) in device_instances.iter().enumerate() {
                let device_label = read_device_label(&client, device_instance).await;
                match scan_device_objects_with_client(
                    &client,
                    device_instance,
                    SCAN_ALL_MAX_OBJECTS_PER_DEVICE,
                )
                .await
                {
                    Ok(objects) => {
                        let count = objects.len();
                        for object in &objects {
                            imported_points.push(point_from_object(object, Some(&device_label)));
                        }
                        all_objects.extend(objects);
                        sender
                            .send(WorkerEvent::Log(
                                LogLevel::Info,
                                format!(
                                    "[{}/{}] device {}: {} object(s)",
                                    idx + 1,
                                    total,
                                    device_instance,
                                    count
                                ),
                            ))
                            .ok();
                    }
                    Err(error) => {
                        failures += 1;
                        sender
                            .send(WorkerEvent::Log(
                                LogLevel::Warning,
                                format!("device {device_instance}: scan failed: {error:#}"),
                            ))
                            .ok();
                    }
                }
                sender
                    .send(WorkerEvent::ScanProgress {
                        current: idx + 1,
                        total,
                    })
                    .ok();
            }
            client.stop().await.ok();

            let scanned = all_objects.len();
            let merge = merge_imported_points(&existing_points, &imported_points);
            let added = merge.added;
            let updated = merge.updated;
            let total_points = merge.points.len();
            sender
                .send(WorkerEvent::BulkTagImport(BulkTagImportOutcome {
                    devices,
                    scanned_objects: all_objects,
                    points: merge.points,
                    added,
                    updated,
                    warnings: Vec::new(),
                }))
                .ok();
            sender
                .send(WorkerEvent::Finished(format!(
                    "Scanned {scanned} object(s) across {total} device(s) ({failures} failure(s)) — {added} point(s) added, {updated} updated, {total_points} total"
                )))
                .ok();
        });
    });
}

pub fn spawn_object_scan(
    sender: Sender<WorkerEvent>,
    config: BacnetConfig,
    interfaces: Vec<NetworkInterface>,
    device_instance: u32,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    format!("Scanning object list for device {device_instance}"),
                ))
                .ok();
            match scan_device_objects(&config, &interfaces, device_instance, 512).await {
                Ok(objects) => {
                    let count = objects.len();
                    sender.send(WorkerEvent::Objects(objects)).ok();
                    sender
                        .send(WorkerEvent::Finished(format!(
                            "Found {count} object(s) on device {device_instance}"
                        )))
                        .ok();
                }
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("Object scan failed: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished("Object scan failed".to_string()))
                        .ok();
                }
            }
        });
    });
}

pub fn spawn_poll_and_publish(
    sender: Sender<WorkerEvent>,
    bacnet: BacnetConfig,
    interfaces: Vec<NetworkInterface>,
    mqtt: MqttConfig,
    points: Vec<PointConfig>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    format!("Polling {} configured point(s)", points.len()),
                ))
                .ok();
            let outcome = match poll_points_once(&bacnet, &interfaces, &mqtt, &points).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("Polling failed: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished("Polling failed".to_string()))
                        .ok();
                    return;
                }
            };

            if !outcome.failures.is_empty() {
                for failure in &outcome.failures {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Warning,
                            format!(
                                "{} read failed: {}",
                                failure.point.display_name(),
                                failure.error
                            ),
                        ))
                        .ok();
                }
            }
            for warning in &outcome.warnings {
                sender
                    .send(WorkerEvent::Log(LogLevel::Warning, warning.clone()))
                    .ok();
            }

            let mut publisher = match RumqttPublisher::new(&mqtt) {
                Ok(publisher) => publisher,
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("MQTT publisher setup failed: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished("Publishing failed".to_string()))
                        .ok();
                    return;
                }
            };
            let stale_points = outcome.failures.len();
            let finished_msg =
                emit_poll_outcome(&sender, &mut publisher, &mqtt, &outcome, stale_points).await;
            sender.send(WorkerEvent::Finished(finished_msg)).ok();
        });
    });
}

pub fn spawn_republisher(
    sender: Sender<WorkerEvent>,
    bacnet: BacnetConfig,
    interfaces: Vec<NetworkInterface>,
    mqtt: MqttConfig,
    points: Vec<PointConfig>,
    stop: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    format!(
                        "Starting continuous republisher for {} point(s)",
                        points.len()
                    ),
                ))
                .ok();

            let mut publisher = match RumqttPublisher::new(&mqtt) {
                Ok(publisher) => publisher,
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("MQTT publisher setup failed: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished(
                            "Continuous republisher failed".to_string(),
                        ))
                        .ok();
                    return;
                }
            };

            // ONE BACnet client for the lifetime of the republisher. Building/tearing down
            // per poll cycle races on the UDP port bind ("Address already in use") because
            // the kernel hasn't released 47808 by the time the next cycle reclaims it.
            let bind = resolve_bacnet_bind_address(&bacnet, &interfaces);
            let mut client = match build_client(&bacnet, bind).await {
                Ok(c) => c,
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Error,
                            format!("Failed to start BACnet client: {error:#}"),
                        ))
                        .ok();
                    sender
                        .send(WorkerEvent::Finished(
                            "Continuous republisher failed".to_string(),
                        ))
                        .ok();
                    return;
                }
            };
            let unique_device_instances = points
                .iter()
                .filter(|p| p.enabled)
                .map(|p| p.device_instance)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let unresolved_devices: HashSet<u32> = match refresh_device_table(
                &client,
                &bacnet,
                &unique_device_instances,
            )
            .await
            {
                Ok(refresh) => {
                    if !refresh.unresolved.is_empty() {
                        sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Warning,
                                    format!(
                                        "{} of {} device(s) not in I-Am cache; their points will be skipped (restart republisher to re-attempt)",
                                        refresh.unresolved.len(),
                                        unique_device_instances.len()
                                    ),
                                ))
                                .ok();
                    }
                    refresh.unresolved.into_iter().collect()
                }
                Err(error) => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Warning,
                            format!("Device table refresh failed: {error:#}"),
                        ))
                        .ok();
                    HashSet::new()
                }
            };

            let mut last_polled = HashMap::<usize, Instant>::new();
            let mut point_statuses = HashMap::<PointIdentity, PointStatus>::new();
            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                let due_points = points
                    .iter()
                    .enumerate()
                    .filter(|(index, point)| {
                        point.enabled
                            && !unresolved_devices.contains(&point.device_instance)
                            && last_polled
                                .get(index)
                                .map(|last| {
                                    now.duration_since(*last)
                                        >= Duration::from_secs(point.poll_interval_secs.max(1))
                                })
                                .unwrap_or(true)
                    })
                    .map(|(index, point)| (index, point.clone()))
                    .collect::<Vec<_>>();

                if !due_points.is_empty() {
                    let poll_set = due_points
                        .iter()
                        .map(|(_, point)| point.clone())
                        .collect::<Vec<_>>();
                    match poll_points_once_with_client(&client, &mqtt, &poll_set).await {
                        Ok(outcome) => {
                            for (index, _) in due_points {
                                last_polled.insert(index, now);
                            }
                            for sample in &outcome.samples {
                                point_statuses
                                    .entry(PointIdentity::from_point(&sample.point))
                                    .or_default()
                                    .record_sample(sample);
                            }
                            for failure in &outcome.failures {
                                point_statuses
                                    .entry(PointIdentity::from_point(&failure.point))
                                    .or_default()
                                    .record_read_failure(failure.error.clone());
                            }
                            let stale_points = point_statuses
                                .values()
                                .filter(|status| status.stale)
                                .count();
                            emit_poll_outcome(
                                &sender,
                                &mut publisher,
                                &mqtt,
                                &outcome,
                                stale_points,
                            )
                            .await;
                        }
                        Err(error) => {
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Error,
                                    format!("Republisher poll failed: {error:#}"),
                                ))
                                .ok();
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            client.stop().await.ok();
            sender
                .send(WorkerEvent::Finished(
                    "Continuous republisher stopped".to_string(),
                ))
                .ok();
        });
    });
}

/// Shared post-poll emit sequence: log failures/warnings, publish samples and health,
/// then send the `Failures`, `Samples`, and `PublishStatus` events to the UI.
/// Returns a human-readable summary string (used by one-shot callers as a `Finished` message).
async fn emit_poll_outcome(
    sender: &Sender<WorkerEvent>,
    publisher: &mut RumqttPublisher,
    mqtt: &MqttConfig,
    outcome: &PollOutcome,
    stale_points: usize,
) -> String {
    for warning in &outcome.warnings {
        sender
            .send(WorkerEvent::Log(LogLevel::Warning, warning.clone()))
            .ok();
    }
    for failure in &outcome.failures {
        sender
            .send(WorkerEvent::Log(
                LogLevel::Warning,
                format!(
                    "{} read failed: {}",
                    failure.point.display_name(),
                    failure.error
                ),
            ))
            .ok();
    }
    let stats = publisher
        .publish_samples_confirmed(mqtt, &outcome.samples)
        .await;
    let _ = publish_health(
        publisher,
        mqtt,
        HealthSnapshot {
            published: stats.published,
            failed_reads: outcome.failures.len(),
            failed_publishes: stats.failed,
            stale_points,
            reconnects: stats.reconnects,
            last_error: stats.last_error.clone(),
        },
    )
    .await;
    sender
        .send(WorkerEvent::Failures(outcome.failures.clone()))
        .ok();
    sender
        .send(WorkerEvent::Samples(outcome.samples.clone()))
        .ok();
    sender.send(WorkerEvent::PublishStatus(stats.clone())).ok();
    format!(
        "Published {} point(s), {} read failure(s), {} publish failure(s)",
        stats.published,
        outcome.failures.len(),
        stats.failed
    )
}

fn run_async<F>(sender: Sender<WorkerEvent>, future: F)
where
    F: std::future::Future<Output = ()>,
{
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime.block_on(future),
        Err(error) => {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Error,
                    format!("Failed to start async runtime: {error}"),
                ))
                .ok();
        }
    }
}
