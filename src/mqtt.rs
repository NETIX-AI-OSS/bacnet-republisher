use crate::config::MqttConfig;
use crate::model::{PointSample, PublishStats};
use anyhow::{Context, Result};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS, Transport};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout;

pub trait MqttPublisher {
    fn publish<'a>(
        &'a mut self,
        topic: &'a str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

pub struct RumqttPublisher {
    client: AsyncClient,
    eventloop: EventLoop,
}

impl RumqttPublisher {
    pub fn new(config: &MqttConfig) -> Self {
        let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.max(5)));
        if config.use_tls {
            options.set_transport(Transport::tls_with_default_config());
        }
        if let Some(username) = config.username.as_deref().filter(|value| !value.is_empty()) {
            options.set_credentials(username, config.password.clone().unwrap_or_default());
        }
        let (client, eventloop) = AsyncClient::new(options, 64);
        Self { client, eventloop }
    }

    pub async fn drain_once(&mut self) {
        let _ = timeout(Duration::from_millis(50), self.eventloop.poll()).await;
    }
}

impl MqttPublisher for RumqttPublisher {
    fn publish<'a>(
        &'a mut self,
        topic: &'a str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .publish(topic, QoS::AtLeastOnce, retain, payload)
                .await
                .with_context(|| format!("failed to enqueue MQTT publish to {topic}"))?;
            self.drain_once().await;
            Ok(())
        })
    }
}

pub async fn publish_samples<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    samples: &[PointSample],
) -> PublishStats {
    let mut stats = PublishStats {
        published: 0,
        failed: 0,
    };

    for sample in samples {
        let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
            Ok(payload) => payload,
            Err(_) => {
                stats.failed += 1;
                continue;
            }
        };

        match publisher
            .publish(&sample.topic, payload, config.retain)
            .await
        {
            Ok(()) => stats.published += 1,
            Err(_) => stats.failed += 1,
        }
    }

    stats
}

pub async fn publish_health<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    published: usize,
    failed_reads: usize,
    failed_publishes: usize,
) -> Result<()> {
    let payload = json!({
        "status": if failed_reads == 0 && failed_publishes == 0 { "ok" } else { "degraded" },
        "published": published,
        "failed_reads": failed_reads,
        "failed_publishes": failed_publishes,
        "timestamp": crate::model::now_millis(),
    });
    publisher
        .publish(
            &config.health_topic,
            serde_json::to_vec(&payload).context("failed to encode health payload")?,
            true,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{PointConfig, TelemetryValue};

    #[derive(Default)]
    struct FakePublisher {
        calls: Vec<(String, Vec<u8>, bool)>,
        fail: bool,
    }

    impl MqttPublisher for FakePublisher {
        fn publish<'a>(
            &'a mut self,
            topic: &'a str,
            payload: Vec<u8>,
            retain: bool,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                if self.fail {
                    anyhow::bail!("publish failed");
                }
                self.calls.push((topic.to_string(), payload, retain));
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn publishes_json_scalars_with_retain_flag() {
        let mut publisher = FakePublisher::default();
        let config = MqttConfig {
            retain: true,
            ..MqttConfig::default()
        };
        let sample = PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Number(22.5),
            topic: "Netix/Site/AHU1/Temp".to_string(),
            timestamp_ms: 1,
        };

        let stats = publish_samples(&mut publisher, &config, &[sample]).await;

        assert_eq!(stats.published, 1);
        assert_eq!(publisher.calls[0].0, "Netix/Site/AHU1/Temp");
        assert_eq!(publisher.calls[0].1, b"22.5");
        assert!(publisher.calls[0].2);
    }

    #[tokio::test]
    async fn health_payload_reports_degraded_state() {
        let mut publisher = FakePublisher::default();
        let config = MqttConfig::default();

        publish_health(&mut publisher, &config, 1, 2, 3)
            .await
            .unwrap();

        let payload: serde_json::Value = serde_json::from_slice(&publisher.calls[0].1).unwrap();
        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["published"], 1);
    }
}
