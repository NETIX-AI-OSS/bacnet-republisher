use crate::bacnet::{
    build_client, discover_devices, point_from_object, poll_points_once,
    poll_points_once_with_client, read_device_label, refresh_device_table, scan_device_objects,
    scan_device_objects_with_client, RefreshOutcome,
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
    RepublisherLifecycle(RepublisherLifecycle),
    Finished(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepublisherLifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
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

            let mut unresolved_scans = 0usize;
            match refresh_device_table(&client, &config, &device_instances).await {
                Ok(refresh) if !refresh.unresolved.is_empty() => {
                    sender
                        .send(WorkerEvent::Log(
                            LogLevel::Warning,
                            format!(
                                "{} of {} device(s) not in I-Am cache after refresh; retrying before each affected scan",
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
                if client.get_device(device_instance).await.is_none() {
                    match refresh_device_table(&client, &config, &[device_instance]).await {
                        Ok(refresh) if refresh.unresolved.contains(&device_instance) => {
                            unresolved_scans += 1;
                            failures += 1;
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Warning,
                                    format!(
                                        "device {device_instance}: unresolved after focused refresh; scan skipped"
                                    ),
                                ))
                                .ok();
                            sender
                                .send(WorkerEvent::ScanProgress {
                                    current: idx + 1,
                                    total,
                                })
                                .ok();
                            continue;
                        }
                        Ok(_) => {
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Info,
                                    format!(
                                        "device {device_instance}: resolved after focused refresh"
                                    ),
                                ))
                                .ok();
                        }
                        Err(error) => {
                            failures += 1;
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Warning,
                                    format!(
                                        "device {device_instance}: focused refresh failed: {error:#}"
                                    ),
                                ))
                                .ok();
                            sender
                                .send(WorkerEvent::ScanProgress {
                                    current: idx + 1,
                                    total,
                                })
                                .ok();
                            continue;
                        }
                    }
                }
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
                    "Scanned {scanned} object(s) across {total} device(s) ({failures} failure(s), {unresolved_scans} unresolved) — {added} point(s) added, {updated} updated, {total_points} total"
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

/// How often the republisher re-broadcasts Who-Is for devices that are not yet
/// in the I-Am cache (missed at startup, rebooted, or readdressed).
const DEVICE_RERESOLVE_INTERVAL: Duration = Duration::from_secs(60);
/// Refresh all known device addresses before bacnet-client's 600s device table TTL.
const DEVICE_TABLE_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(240);
/// Upper bound on BACnet client shutdown so a hung transport can't wedge the
/// lifecycle in `Stopping`.
const CLIENT_STOP_TIMEOUT: Duration = Duration::from_secs(5);
/// First retry delay for a device whose reads all failed; doubles per failed
/// attempt up to the configured `device_backoff_max_secs`.
const DEVICE_BACKOFF_INITIAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
struct DeviceBackoff {
    delay: Duration,
    until: Instant,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RefreshStateChange {
    newly_resolved: Vec<u32>,
    newly_unresolved: HashSet<u32>,
}

/// Escalates backoff for devices where every read failed this cycle and clears
/// it for devices that produced at least one sample. Returns log messages for
/// the caller to emit.
fn update_device_backoffs(
    backoffs: &mut HashMap<u32, DeviceBackoff>,
    polled_devices: &HashSet<u32>,
    outcome: &PollOutcome,
    now: Instant,
    max_delay: Duration,
) -> Vec<(LogLevel, String)> {
    let healthy = outcome
        .samples
        .iter()
        .map(|sample| sample.point.device_instance)
        .collect::<HashSet<_>>();
    let failed = outcome
        .failures
        .iter()
        .map(|failure| failure.point.device_instance)
        .collect::<HashSet<_>>();

    let mut messages = Vec::new();
    for &device in polled_devices {
        if healthy.contains(&device) {
            if backoffs.remove(&device).is_some() {
                messages.push((
                    LogLevel::Info,
                    format!("device {device} responding again; backoff cleared"),
                ));
            }
        } else if failed.contains(&device) {
            let delay = match backoffs.get(&device) {
                Some(backoff) => backoff.delay.saturating_mul(2).min(max_delay),
                None => DEVICE_BACKOFF_INITIAL.min(max_delay),
            };
            backoffs.insert(
                device,
                DeviceBackoff {
                    delay,
                    until: now + delay,
                },
            );
            messages.push((
                LogLevel::Warning,
                format!(
                    "device {device}: all reads failed; next attempt in {}s",
                    delay.as_secs()
                ),
            ));
        }
        // Neither sampled nor failed: the cycle was cancelled before this device
        // was reached — leave its backoff state untouched.
    }
    messages
}

fn apply_refresh_state(
    unresolved_devices: &mut HashSet<u32>,
    device_backoffs: &mut HashMap<u32, DeviceBackoff>,
    refresh: &RefreshOutcome,
) -> RefreshStateChange {
    let mut change = RefreshStateChange::default();

    for &device in &refresh.resolved {
        if unresolved_devices.remove(&device) {
            change.newly_resolved.push(device);
        }
        device_backoffs.remove(&device);
    }
    change.newly_resolved.sort_unstable();
    change.newly_resolved.dedup();

    for &device in &refresh.unresolved {
        if unresolved_devices.insert(device) {
            change.newly_unresolved.insert(device);
        }
    }

    change
}

fn emit_refresh_state_change(
    sender: &Sender<WorkerEvent>,
    points: &[PointConfig],
    label: &str,
    change: RefreshStateChange,
    point_statuses: &mut HashMap<PointIdentity, PointStatus>,
) {
    if !change.newly_resolved.is_empty() {
        sender
            .send(WorkerEvent::Log(
                LogLevel::Info,
                format!(
                    "{} device(s) resolved during {label}: {:?}",
                    change.newly_resolved.len(),
                    change.newly_resolved
                ),
            ))
            .ok();
    }
    if !change.newly_unresolved.is_empty() {
        let mut newly_unresolved = change.newly_unresolved.into_iter().collect::<Vec<_>>();
        newly_unresolved.sort_unstable();
        sender
            .send(WorkerEvent::Log(
                LogLevel::Warning,
                format!(
                    "{} device(s) unresolved during {label}: {:?}",
                    newly_unresolved.len(),
                    newly_unresolved
                ),
            ))
            .ok();
        let newly_unresolved_set = newly_unresolved.iter().copied().collect::<HashSet<_>>();
        record_unresolved_failures(sender, points, &newly_unresolved_set, point_statuses);
    }
}

/// Mark every enabled point on an unresolved device as failed, so the UI and the
/// MQTT health snapshot report them stale instead of silently skipping them.
fn record_unresolved_failures(
    sender: &Sender<WorkerEvent>,
    points: &[PointConfig],
    unresolved_devices: &HashSet<u32>,
    point_statuses: &mut HashMap<PointIdentity, PointStatus>,
) {
    if unresolved_devices.is_empty() {
        return;
    }
    let failures = points
        .iter()
        .filter(|point| point.enabled && unresolved_devices.contains(&point.device_instance))
        .map(|point| PointFailure {
            point: point.clone(),
            error: format!("device {} not in I-Am cache", point.device_instance),
        })
        .collect::<Vec<_>>();
    if failures.is_empty() {
        return;
    }
    for failure in &failures {
        point_statuses
            .entry(PointIdentity::from_point(&failure.point))
            .or_default()
            .record_read_failure(failure.error.clone());
    }
    sender.send(WorkerEvent::Failures(failures)).ok();
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
        let runtime_sender = sender.clone();
        let future_sender = sender.clone();
        let completed = run_async(runtime_sender, async move {
            let sender = future_sender;
            sender
                .send(WorkerEvent::RepublisherLifecycle(
                    RepublisherLifecycle::Starting,
                ))
                .ok();
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
                    let message = format!("MQTT publisher setup failed: {error:#}");
                    sender
                        .send(WorkerEvent::Log(LogLevel::Error, message.clone()))
                        .ok();
                    sender
                        .send(WorkerEvent::RepublisherLifecycle(
                            RepublisherLifecycle::Failed(message),
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
                    let message = format!("Failed to start BACnet client: {error:#}");
                    sender
                        .send(WorkerEvent::Log(LogLevel::Error, message.clone()))
                        .ok();
                    sender
                        .send(WorkerEvent::RepublisherLifecycle(
                            RepublisherLifecycle::Failed(message),
                        ))
                        .ok();
                    return;
                }
            };
            sender
                .send(WorkerEvent::RepublisherLifecycle(
                    RepublisherLifecycle::Running,
                ))
                .ok();
            let mut unique_device_instances = points
                .iter()
                .filter(|p| p.enabled)
                .map(|p| p.device_instance)
                .collect::<Vec<_>>();
            unique_device_instances.sort_unstable();
            unique_device_instances.dedup();
            let mut point_statuses = HashMap::<PointIdentity, PointStatus>::new();
            let mut unresolved_devices: HashSet<u32> = match refresh_device_table(
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
                                        "{} of {} device(s) not in I-Am cache; their points will be skipped (resolution retried every {}s)",
                                        refresh.unresolved.len(),
                                        unique_device_instances.len(),
                                        DEVICE_RERESOLVE_INTERVAL.as_secs()
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
            record_unresolved_failures(&sender, &points, &unresolved_devices, &mut point_statuses);
            let mut last_resolve_attempt = Instant::now();
            let mut last_full_refresh = Instant::now();

            let mut last_polled = HashMap::<usize, Instant>::new();
            let mut device_backoffs = HashMap::<u32, DeviceBackoff>::new();
            let device_backoff_max = Duration::from_secs(bacnet.device_backoff_max_secs.max(10));
            while !stop.load(Ordering::Relaxed) {
                let mut refreshed_this_iteration = false;
                if last_full_refresh.elapsed() >= DEVICE_TABLE_KEEPALIVE_INTERVAL {
                    last_full_refresh = Instant::now();
                    last_resolve_attempt = Instant::now();
                    refreshed_this_iteration = true;
                    match refresh_device_table(&client, &bacnet, &unique_device_instances).await {
                        Ok(refresh) => {
                            let change = apply_refresh_state(
                                &mut unresolved_devices,
                                &mut device_backoffs,
                                &refresh,
                            );
                            emit_refresh_state_change(
                                &sender,
                                &points,
                                "device table keepalive",
                                change,
                                &mut point_statuses,
                            );
                        }
                        Err(error) => {
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Warning,
                                    format!("Device table keepalive failed: {error:#}"),
                                ))
                                .ok();
                        }
                    }
                }

                // Devices that missed the startup Who-Is window (or rebooted with a new
                // address) get periodic re-resolution attempts instead of being skipped
                // until the republisher is restarted.
                if !refreshed_this_iteration
                    && !unresolved_devices.is_empty()
                    && last_resolve_attempt.elapsed() >= DEVICE_RERESOLVE_INTERVAL
                {
                    last_resolve_attempt = Instant::now();
                    let targets = unresolved_devices.iter().copied().collect::<Vec<_>>();
                    match refresh_device_table(&client, &bacnet, &targets).await {
                        Ok(refresh) => {
                            let change = apply_refresh_state(
                                &mut unresolved_devices,
                                &mut device_backoffs,
                                &refresh,
                            );
                            emit_refresh_state_change(
                                &sender,
                                &points,
                                "device re-resolution",
                                change,
                                &mut point_statuses,
                            );
                        }
                        Err(error) => {
                            sender
                                .send(WorkerEvent::Log(
                                    LogLevel::Warning,
                                    format!("Device re-resolution failed: {error:#}"),
                                ))
                                .ok();
                        }
                    }
                }

                let now = Instant::now();
                let due_points = points
                    .iter()
                    .enumerate()
                    .filter(|(index, point)| {
                        point.enabled
                            && !unresolved_devices.contains(&point.device_instance)
                            && device_backoffs
                                .get(&point.device_instance)
                                .is_none_or(|backoff| now >= backoff.until)
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
                    match poll_points_once_with_client(
                        &client,
                        &mqtt,
                        &poll_set,
                        bacnet.poll_concurrency,
                        Some(&stop),
                    )
                    .await
                    {
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
                            let polled_devices = poll_set
                                .iter()
                                .map(|point| point.device_instance)
                                .collect::<HashSet<_>>();
                            for (level, message) in update_device_backoffs(
                                &mut device_backoffs,
                                &polled_devices,
                                &outcome,
                                now,
                                device_backoff_max,
                            ) {
                                sender.send(WorkerEvent::Log(level, message)).ok();
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

            sender
                .send(WorkerEvent::RepublisherLifecycle(
                    RepublisherLifecycle::Stopping,
                ))
                .ok();
            if tokio::time::timeout(CLIENT_STOP_TIMEOUT, client.stop())
                .await
                .is_err()
            {
                sender
                    .send(WorkerEvent::Log(
                        LogLevel::Warning,
                        format!(
                            "BACnet client did not stop within {}s; abandoning it",
                            CLIENT_STOP_TIMEOUT.as_secs()
                        ),
                    ))
                    .ok();
            }
            sender
                .send(WorkerEvent::RepublisherLifecycle(
                    RepublisherLifecycle::Stopped,
                ))
                .ok();
            sender
                .send(WorkerEvent::Finished(
                    "Continuous republisher stopped".to_string(),
                ))
                .ok();
        });
        // Covers both runtime startup failure and a panic anywhere in the worker —
        // without this the UI would stay in Starting/Running/Stopping forever with
        // no way to restart.
        if !completed {
            sender
                .send(WorkerEvent::RepublisherLifecycle(
                    RepublisherLifecycle::Failed(
                        "Republisher worker stopped unexpectedly; see log for details".to_string(),
                    ),
                ))
                .ok();
        }
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
    let stats = publisher.enqueue_samples(mqtt, &outcome.samples);
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

/// Runs the worker future to completion, returning false if the runtime could not
/// start or the future panicked. A panicking worker must not die silently: the UI
/// cannot observe thread death (it holds its own sender clone, so the channel never
/// disconnects) and would otherwise show a stale state forever.
fn run_async<F>(sender: Sender<WorkerEvent>, future: F) -> bool
where
    F: std::future::Future<Output = ()>,
{
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Error,
                    format!("Failed to start async runtime: {error:#}"),
                ))
                .ok();
            return false;
        }
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(future))) {
        Ok(()) => true,
        Err(panic) => {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Error,
                    format!("Worker thread crashed: {}", panic_message(panic.as_ref())),
                ))
                .ok();
            sender
                .send(WorkerEvent::Finished(
                    "Worker stopped unexpectedly".to_string(),
                ))
                .ok();
            false
        }
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;

    fn point_on_device(device_instance: u32, enabled: bool) -> PointConfig {
        PointConfig {
            device_instance,
            enabled,
            ..PointConfig::default()
        }
    }

    #[test]
    fn unresolved_devices_mark_points_stale_and_emit_failures() {
        let (sender, receiver) = unbounded();
        let points = vec![
            point_on_device(100, true),
            point_on_device(200, true),
            point_on_device(100, false),
        ];
        let unresolved = HashSet::from([100]);
        let mut statuses = HashMap::new();

        record_unresolved_failures(&sender, &points, &unresolved, &mut statuses);

        assert_eq!(statuses.len(), 1);
        let status = statuses.values().next().unwrap();
        assert!(status.stale);
        assert!(status.last_error.as_deref().unwrap().contains("100"));

        let event = receiver.try_recv().unwrap();
        match event {
            WorkerEvent::Failures(failures) => {
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].point.device_instance, 100);
            }
            other => panic!("expected Failures event, got {other:?}"),
        }
    }

    #[test]
    fn fully_resolved_devices_emit_nothing() {
        let (sender, receiver) = unbounded();
        let points = vec![point_on_device(100, true)];
        let mut statuses = HashMap::new();

        record_unresolved_failures(&sender, &points, &HashSet::new(), &mut statuses);

        assert!(statuses.is_empty());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn refresh_state_adds_and_removes_unresolved_devices_and_clears_backoff() {
        let mut unresolved = HashSet::from([100, 200]);
        let now = Instant::now();
        let mut backoffs = HashMap::from([
            (
                100,
                DeviceBackoff {
                    delay: Duration::from_secs(10),
                    until: now,
                },
            ),
            (
                300,
                DeviceBackoff {
                    delay: Duration::from_secs(10),
                    until: now,
                },
            ),
        ]);
        let refresh = RefreshOutcome {
            resolved: vec![100, 300],
            unresolved: vec![200, 400],
        };

        let change = apply_refresh_state(&mut unresolved, &mut backoffs, &refresh);

        assert_eq!(change.newly_resolved, vec![100]);
        assert_eq!(change.newly_unresolved, HashSet::from([400]));
        assert_eq!(unresolved, HashSet::from([200, 400]));
        assert!(!backoffs.contains_key(&100));
        assert!(!backoffs.contains_key(&300));
    }

    fn outcome_with(samples_for: &[u32], failures_for: &[u32]) -> PollOutcome {
        PollOutcome {
            samples: samples_for
                .iter()
                .map(|&device_instance| PointSample {
                    point: point_on_device(device_instance, true),
                    topic: "t".to_string(),
                    value: crate::model::TelemetryValue::Number(1.0),
                    timestamp_ms: 0,
                })
                .collect(),
            failures: failures_for
                .iter()
                .map(|&device_instance| PointFailure {
                    point: point_on_device(device_instance, true),
                    error: "timeout".to_string(),
                })
                .collect(),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn failing_device_backs_off_exponentially_up_to_cap() {
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([100]);
        let now = Instant::now();
        let max = Duration::from_secs(30);

        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[], &[100]), now, max);
        assert_eq!(backoffs[&100].delay, Duration::from_secs(10));
        assert_eq!(backoffs[&100].until, now + Duration::from_secs(10));

        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[], &[100]), now, max);
        assert_eq!(backoffs[&100].delay, Duration::from_secs(20));

        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[], &[100]), now, max);
        assert_eq!(backoffs[&100].delay, Duration::from_secs(30));

        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[], &[100]), now, max);
        assert_eq!(backoffs[&100].delay, Duration::from_secs(30));
    }

    #[test]
    fn successful_sample_clears_backoff_even_with_partial_failures() {
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([100]);
        let now = Instant::now();
        let max = Duration::from_secs(300);

        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[], &[100]), now, max);
        assert!(backoffs.contains_key(&100));

        // One good sample means the device is alive, even if other points failed.
        let messages = update_device_backoffs(
            &mut backoffs,
            &polled,
            &outcome_with(&[100], &[100]),
            now,
            max,
        );
        assert!(backoffs.is_empty());
        assert!(messages
            .iter()
            .any(|(_, message)| message.contains("responding again")));
    }

    #[test]
    fn cancelled_cycle_leaves_unreached_devices_untouched() {
        let mut backoffs = HashMap::new();
        let polled = HashSet::from([100, 200]);
        let now = Instant::now();
        let max = Duration::from_secs(300);

        // Device 200 was in the poll set but produced neither samples nor
        // failures (cycle cancelled before it was reached).
        update_device_backoffs(&mut backoffs, &polled, &outcome_with(&[100], &[]), now, max);
        assert!(!backoffs.contains_key(&200));
    }

    #[test]
    fn run_async_reports_panics_instead_of_dying_silently() {
        let (sender, receiver) = unbounded();

        let completed = run_async(sender, async {
            panic!("boom");
        });

        assert!(!completed);
        let events = receiver.try_iter().collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(
            event,
            WorkerEvent::Log(LogLevel::Error, message) if message.contains("boom")
        )));
        assert!(events
            .iter()
            .any(|event| matches!(event, WorkerEvent::Finished(_))));
    }
}
