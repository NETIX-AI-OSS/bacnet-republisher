//! Manual poll test against local BACnet simulator (RPM + presentValue).
use bacnet_republisher::bacnet::poll_points_once;
use bacnet_republisher::config::{BacnetConfig, MqttConfig};
use bacnet_republisher::network::ipv4_interfaces;
use bacnet_republisher::seed::simulator_points;

#[tokio::test]
#[ignore = "requires local BACnet simulator on UDP/47808"]
async fn polls_simulator_points_via_rpm() {
    let mut config = BacnetConfig {
        discover_all_interfaces: false,
        ..BacnetConfig::default()
    };
    let interfaces = ipv4_interfaces();
    if let Some(bridge) = interfaces
        .iter()
        .find(|iface| iface.name.starts_with("bridge"))
    {
        config.selected_interface = Some(bridge.addr);
    }

    let points = simulator_points()
        .into_iter()
        .filter(|point| point.device_instance == 10700)
        .collect::<Vec<_>>();

    let outcome = poll_points_once(&config, &interfaces, &MqttConfig::default(), &points)
        .await
        .expect("poll should succeed");

    eprintln!(
        "samples={} failures={} warnings={:?}",
        outcome.samples.len(),
        outcome.failures.len(),
        outcome.warnings
    );
    for sample in &outcome.samples {
        eprintln!("  {} -> {}", sample.topic, sample.value);
    }

    assert_eq!(outcome.failures.len(), 0);
    assert_eq!(outcome.warnings.len(), 0);
    assert_eq!(outcome.samples.len(), 3);
}
