//! Manual object scan against local BACnet simulator.
use bacnet_republisher::bacnet::scan_device_objects;
use bacnet_republisher::config::BacnetConfig;
use bacnet_republisher::network::ipv4_interfaces;

#[tokio::test]
#[ignore = "requires local BACnet simulator on UDP/47808 with ReadProperty support"]
async fn scans_objects_on_simulator_device_ahu_l_001() {
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

    // AHU-L-001 = device_id 10700, first instance in the ahu_large template block.
    let objects = scan_device_objects(&config, &interfaces, 10700, 32)
        .await
        .expect("object scan should succeed");

    eprintln!("scanned {} object(s)", objects.len());
    for object in &objects {
        eprintln!(
            "  {} {} {:?}",
            object.object_type, object.object_instance, object.present_value
        );
    }

    assert!(
        objects.len() >= 16,
        "AHU-L-001 should expose all 16 points from the ahu_large template"
    );
}
