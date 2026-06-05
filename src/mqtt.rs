use crate::config::MqttConfig;
use crate::model::{PointSample, PublishStats};
use anyhow::{anyhow, Context, Result};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS, TlsConfiguration, Transport};
use serde_json::json;
use std::fs;
use std::future::Future;
use std::io::BufReader;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const DRAIN_SLICE: Duration = Duration::from_millis(250);
const PUBLISH_DRAIN: Duration = Duration::from_secs(5);
const INTERLEAVE_DRAIN: Duration = Duration::from_millis(150);
const INTERLEAVE_EVERY: usize = 16;
const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
// rumqttc's outbound channel must hold the entire burst — publish() blocks if it fills.
// Sized for full-fleet bursts (~1500 points) with comfortable headroom.
const OUTBOUND_CHANNEL_CAPACITY: usize = 4096;

pub trait MqttPublisher {
    fn publish<'a>(
        &'a mut self,
        topic: &'a str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectBackoff {
    current: Duration,
    max: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            current: BACKOFF_INITIAL,
            max: BACKOFF_MAX,
        }
    }
}

impl ReconnectBackoff {
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }

    pub fn reset(&mut self) {
        self.current = BACKOFF_INITIAL;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthSnapshot {
    pub published: usize,
    pub failed_reads: usize,
    pub failed_publishes: usize,
    pub stale_points: usize,
    pub reconnects: usize,
    pub last_error: Option<String>,
}

impl HealthSnapshot {
    pub fn status(&self) -> &'static str {
        if self.failed_reads == 0 && self.failed_publishes == 0 && self.stale_points == 0 {
            "ok"
        } else {
            "degraded"
        }
    }
}

pub struct RumqttPublisher {
    client: AsyncClient,
    eventloop: EventLoop,
    backoff: ReconnectBackoff,
}

impl RumqttPublisher {
    pub fn new(config: &MqttConfig) -> Result<Self> {
        let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs.max(5)));
        options.set_transport(build_transport(config)?);
        if let Some(username) = config.username.as_deref().filter(|value| !value.is_empty()) {
            options.set_credentials(username, config.password.clone().unwrap_or_default());
        }
        let (client, eventloop) = AsyncClient::new(options, OUTBOUND_CHANNEL_CAPACITY);
        Ok(Self {
            client,
            eventloop,
            backoff: ReconnectBackoff::default(),
        })
    }

    pub async fn drain_for(&mut self, duration: Duration) -> Result<PublishStats> {
        let mut stats = PublishStats::empty();
        let deadline = Instant::now() + duration;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let poll_for = remaining.min(DRAIN_SLICE);
            match timeout(poll_for, self.eventloop.poll()).await {
                Ok(Ok(_event)) => self.backoff.reset(),
                Ok(Err(error)) => {
                    stats.reconnects += 1;
                    let error = error.to_string();
                    stats.last_error = Some(error.clone());
                    let delay = self.backoff.next_delay();
                    sleep(delay.min(remaining)).await;
                    return Err(anyhow!("MQTT event loop failed: {error}"));
                }
                Err(_) => break,
            }
        }

        Ok(stats)
    }

    pub async fn publish_samples_confirmed(
        &mut self,
        config: &MqttConfig,
        samples: &[PointSample],
    ) -> PublishStats {
        let mut stats = PublishStats::empty();
        let mut since_drain = 0usize;
        for sample in samples {
            stats.queued += 1;
            let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
                Ok(payload) => payload,
                Err(error) => {
                    stats.record_failure(error.to_string());
                    continue;
                }
            };

            if let Err(error) = self
                .client
                .publish(&sample.topic, QoS::AtLeastOnce, config.retain, payload)
                .await
            {
                stats.record_failure(error.to_string());
            }

            // Interleave a short eventloop drain every N publishes so the outbound
            // channel never sits full. Without this, large bursts (1000+ samples) can
            // saturate the channel before the eventloop processes anything, and the
            // next publish().await blocks indefinitely.
            since_drain += 1;
            if since_drain >= INTERLEAVE_EVERY {
                since_drain = 0;
                if let Ok(drain_stats) = self.drain_for(INTERLEAVE_DRAIN).await {
                    stats.reconnects += drain_stats.reconnects;
                    if drain_stats.last_error.is_some() {
                        stats.last_error = drain_stats.last_error;
                    }
                }
            }
        }

        // Final drain to flush any remaining queued publishes.
        if stats.failed == 0 {
            match self.drain_for(PUBLISH_DRAIN).await {
                Ok(drain_stats) => {
                    stats.reconnects += drain_stats.reconnects;
                    if drain_stats.last_error.is_some() {
                        stats.last_error = drain_stats.last_error;
                    }
                    stats.published = stats.queued;
                }
                Err(error) => {
                    stats.record_failure(error.to_string());
                }
            }
        }

        stats
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
            self.drain_for(PUBLISH_DRAIN).await?;
            Ok(())
        })
    }
}

pub async fn publish_samples<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    samples: &[PointSample],
) -> PublishStats {
    let mut stats = PublishStats::empty();

    for sample in samples {
        stats.queued += 1;
        let payload = match serde_json::to_vec(&sample.value.as_json_value()) {
            Ok(payload) => payload,
            Err(error) => {
                stats.record_failure(error.to_string());
                continue;
            }
        };

        match publisher
            .publish(&sample.topic, payload, config.retain)
            .await
        {
            Ok(()) => stats.published += 1,
            Err(error) => stats.record_failure(error.to_string()),
        }
    }

    stats
}

pub async fn publish_health<P: MqttPublisher + Send>(
    publisher: &mut P,
    config: &MqttConfig,
    snapshot: HealthSnapshot,
) -> Result<()> {
    let payload = json!({
        "status": snapshot.status(),
        "published": snapshot.published,
        "failed_reads": snapshot.failed_reads,
        "failed_publishes": snapshot.failed_publishes,
        "stale_points": snapshot.stale_points,
        "reconnects": snapshot.reconnects,
        "last_error": snapshot.last_error,
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

fn build_transport(config: &MqttConfig) -> Result<Transport> {
    if !config.use_tls {
        return Ok(Transport::tcp());
    }

    let client_auth = match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert_path), Some(key_path)) => Some((
            load_cert_chain(Path::new(cert_path))
                .with_context(|| format!("failed to load MQTT client certificate {cert_path}"))?,
            load_private_key(Path::new(key_path))
                .with_context(|| format!("failed to load MQTT client key {key_path}"))?,
        )),
        _ => None,
    };

    if client_auth.is_none() && config.ca_cert_path.is_none() {
        return Ok(Transport::tls_with_default_config());
    }

    let roots = if let Some(ca_path) = &config.ca_cert_path {
        load_root_store_from_file(Path::new(ca_path))
            .with_context(|| format!("failed to load MQTT CA certificate {ca_path}"))?
    } else {
        load_native_root_store().context("failed to load platform TLS root certificates")?
    };

    let builder =
        rumqttc::tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots);
    let tls_config = if let Some((certs, key)) = client_auth {
        builder
            .with_client_auth_cert(certs, key)
            .context("failed to configure MQTT client certificate")?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Transport::tls_with_config(TlsConfiguration::Rustls(
        Arc::new(tls_config),
    )))
}

fn load_root_store_from_file(path: &Path) -> Result<rumqttc::tokio_rustls::rustls::RootCertStore> {
    let mut roots = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
    let certs = load_cert_chain(path)?;
    let (added, ignored) = roots.add_parsable_certificates(certs);
    if added == 0 {
        return Err(anyhow!(
            "no usable CA certificates found; ignored {ignored}"
        ));
    }
    Ok(roots)
}

fn load_native_root_store() -> Result<rumqttc::tokio_rustls::rustls::RootCertStore> {
    let mut roots = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    for cert in result.certs {
        roots
            .add(cert)
            .context("failed to add native TLS root certificate")?;
    }
    if roots.is_empty() {
        return Err(anyhow!(
            "no native TLS root certificates loaded: {:?}",
            result.errors
        ));
    }
    Ok(roots)
}

fn load_cert_chain(
    path: &Path,
) -> Result<Vec<rumqttc::tokio_rustls::rustls::pki_types::CertificateDer<'static>>> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(raw.as_slice());
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("failed to parse certificates from {}", path.display()))
}

fn load_private_key(
    path: &Path,
) -> Result<rumqttc::tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(raw.as_slice());
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("failed to parse private key from {}", path.display()))?
        .ok_or_else(|| anyhow!("no private key found in {}", path.display()))
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

        assert_eq!(stats.queued, 1);
        assert_eq!(stats.published, 1);
        assert_eq!(publisher.calls[0].0, "Netix/Site/AHU1/Temp");
        assert_eq!(publisher.calls[0].1, b"22.5");
        assert!(publisher.calls[0].2);
    }

    #[tokio::test]
    async fn health_payload_reports_degraded_state() {
        let mut publisher = FakePublisher::default();
        let config = MqttConfig::default();

        publish_health(
            &mut publisher,
            &config,
            HealthSnapshot {
                published: 1,
                failed_reads: 2,
                failed_publishes: 3,
                stale_points: 4,
                reconnects: 5,
                last_error: Some("network".to_string()),
            },
        )
        .await
        .unwrap();

        let payload: serde_json::Value = serde_json::from_slice(&publisher.calls[0].1).unwrap();
        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["published"], 1);
        assert_eq!(payload["stale_points"], 4);
        assert_eq!(payload["reconnects"], 5);
    }

    #[test]
    fn reconnect_backoff_caps_and_resets() {
        let mut backoff = ReconnectBackoff::default();

        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(), Duration::from_secs(2));
        for _ in 0..10 {
            backoff.next_delay();
        }
        assert_eq!(backoff.next_delay(), Duration::from_secs(30));

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn failed_publish_updates_counters() {
        let mut publisher = FakePublisher {
            fail: true,
            ..FakePublisher::default()
        };
        let sample = PointSample {
            point: PointConfig::default(),
            value: TelemetryValue::Text("active".to_string()),
            topic: "Netix/Site/AHU1/mode".to_string(),
            timestamp_ms: 1,
        };

        let stats = publish_samples(&mut publisher, &MqttConfig::default(), &[sample]).await;

        assert_eq!(stats.queued, 1);
        assert_eq!(stats.published, 0);
        assert_eq!(stats.failed, 1);
        assert!(stats
            .last_error
            .as_deref()
            .unwrap()
            .contains("publish failed"));
    }
}
