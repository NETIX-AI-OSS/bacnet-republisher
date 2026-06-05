use crate::bacnet::{discover_devices, poll_points_once, scan_device_objects};
use crate::config::{BacnetConfig, MqttConfig};
use crate::log::LogLevel;
use crate::model::{DeviceObject, DiscoveredDevice, NetworkInterface, PointConfig, PointSample};
use crate::mqtt::{publish_health, publish_samples, RumqttPublisher};
use crossbeam_channel::Sender;
use std::collections::HashMap;
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
    Samples(Vec<PointSample>),
    PublishStatus { published: usize, failed: usize },
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
                Ok(devices) => {
                    let count = devices.len();
                    sender.send(WorkerEvent::Devices(devices)).ok();
                    sender
                        .send(WorkerEvent::Finished(format!(
                            "Discovered {count} BACnet device(s)"
                        )))
                        .ok();
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

pub fn spawn_object_scan(sender: Sender<WorkerEvent>, config: BacnetConfig, device_instance: u32) {
    std::thread::spawn(move || {
        run_async(sender.clone(), async move {
            sender
                .send(WorkerEvent::Log(
                    LogLevel::Info,
                    format!("Scanning object list for device {device_instance}"),
                ))
                .ok();
            match scan_device_objects(&config, device_instance, 512).await {
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
            let outcome = match poll_points_once(&bacnet, &mqtt, &points).await {
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

            let mut publisher = RumqttPublisher::new(&mqtt);
            let stats = publish_samples(&mut publisher, &mqtt, &outcome.samples).await;
            let _ = publish_health(
                &mut publisher,
                &mqtt,
                stats.published,
                outcome.failures.len(),
                stats.failed,
            )
            .await;

            sender.send(WorkerEvent::Samples(outcome.samples)).ok();
            sender
                .send(WorkerEvent::PublishStatus {
                    published: stats.published,
                    failed: stats.failed,
                })
                .ok();
            sender
                .send(WorkerEvent::Finished(format!(
                    "Published {} point(s), {} read failure(s), {} publish failure(s)",
                    stats.published,
                    outcome.failures.len(),
                    stats.failed
                )))
                .ok();
        });
    });
}

pub fn spawn_republisher(
    sender: Sender<WorkerEvent>,
    bacnet: BacnetConfig,
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

            let mut last_polled = HashMap::<usize, Instant>::new();
            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                let due_points = points
                    .iter()
                    .enumerate()
                    .filter(|(index, point)| {
                        point.enabled
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
                    match poll_points_once(&bacnet, &mqtt, &poll_set).await {
                        Ok(outcome) => {
                            for (index, _) in due_points {
                                last_polled.insert(index, now);
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
                            let mut publisher = RumqttPublisher::new(&mqtt);
                            let stats =
                                publish_samples(&mut publisher, &mqtt, &outcome.samples).await;
                            let _ = publish_health(
                                &mut publisher,
                                &mqtt,
                                stats.published,
                                outcome.failures.len(),
                                stats.failed,
                            )
                            .await;
                            sender.send(WorkerEvent::Samples(outcome.samples)).ok();
                            sender
                                .send(WorkerEvent::PublishStatus {
                                    published: stats.published,
                                    failed: stats.failed,
                                })
                                .ok();
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
                .send(WorkerEvent::Finished(
                    "Continuous republisher stopped".to_string(),
                ))
                .ok();
        });
    });
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
