//! Manual smoke test against a local BACnet simulator on UDP/47808.
use bacnet_republisher::bacnet::discover_devices;
use bacnet_republisher::config::BacnetConfig;
use bacnet_republisher::network::ipv4_interfaces;

#[tokio::test]
#[ignore = "requires local BACnet simulator on UDP/47808"]
async fn discovers_simulator_devices_with_default_policy() {
    let config = BacnetConfig::default();
    let interfaces = ipv4_interfaces();
    eprintln!("discovery interfaces: {interfaces:?}");

    let outcome = discover_devices(&config, &interfaces)
        .await
        .expect("discovery should complete when at least one NIC binds");

    for warning in &outcome.warnings {
        eprintln!("warning: {warning}");
    }
    eprintln!("discovered {} device(s)", outcome.devices.len());
    for device in &outcome.devices {
        eprintln!(
            "  #{} @ {} (vendor {})",
            device.instance, device.address, device.vendor_id
        );
    }

    assert!(
        !outcome.devices.is_empty(),
        "expected at least one simulator device (block-allocated starting at 10100)"
    );
}
