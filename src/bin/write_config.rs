//! Writes a republisher config aligned with the BACnet building simulator.
use bacnet_republisher::config::{save_to_path, AppConfig, DiscoveryBindFailurePolicy};
use bacnet_republisher::network::ipv4_interfaces;
use bacnet_republisher::seed::simulator_points;

fn main() -> anyhow::Result<()> {
    let path = bacnet_republisher::config::config_path()?;
    let mut config = AppConfig::default();
    config.bacnet.discover_all_interfaces = true;
    config.bacnet.discovery_bind_failure_policy = DiscoveryBindFailurePolicy::Skip;
    config.mqtt.use_tls = false;
    config.mqtt.port = 1883;
    config.points = simulator_points();

    let interfaces = ipv4_interfaces();
    if let Some(preferred) = interfaces
        .iter()
        .find(|iface| iface.name.starts_with("bridge"))
    {
        config.bacnet.selected_interface = Some(preferred.addr);
        eprintln!(
            "Set bacnet.selected_interface to {} ({}) for Docker/OrbStack reachability",
            preferred.addr, preferred.name
        );
    } else if let Some(first) = interfaces.first() {
        eprintln!(
            "No bridge NIC found; poll/scan will bind via first discovery interface {} ({})",
            first.addr, first.name
        );
    }

    save_to_path(&path, &config)?;
    eprintln!(
        "Wrote {} with {} simulator point(s)",
        path.display(),
        config.points.len()
    );
    Ok(())
}
