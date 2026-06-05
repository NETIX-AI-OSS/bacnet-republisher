use crate::config::BacnetConfig;
use crate::model::NetworkInterface;
use get_if_addrs::{get_if_addrs, IfAddr};
use std::net::Ipv4Addr;

/// Returns whether an interface is suitable for BACnet/IP discovery binding.
pub fn is_bacnet_discovery_interface(name: &str, addr: Ipv4Addr) -> bool {
    if addr.is_loopback() || addr.is_unspecified() || addr.is_link_local() {
        return false;
    }

    let lower = name.to_ascii_lowercase();
    const EXCLUDED_PREFIXES: &[&str] = &[
        "utun", "ppp", "ipsec", "gif", "stf", "awdl", "llw", "lo", "ap",
    ];
    !EXCLUDED_PREFIXES
        .iter()
        .any(|prefix| lower == *prefix || lower.starts_with(prefix))
}

pub fn ipv4_interfaces() -> Vec<NetworkInterface> {
    let mut interfaces = get_if_addrs()
        .map(|interfaces| {
            interfaces
                .into_iter()
                .filter_map(|interface| match interface.addr {
                    IfAddr::V4(v4) if is_bacnet_discovery_interface(&interface.name, v4.ip) => {
                        Some(NetworkInterface {
                            name: interface.name,
                            addr: v4.ip,
                        })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    interfaces.sort_by(|left, right| left.name.cmp(&right.name).then(left.addr.cmp(&right.addr)));
    interfaces.dedup_by(|left, right| left.name == right.name && left.addr == right.addr);
    interfaces
}

/// Preferred local IPv4 for poll/scan when the operator has not chosen a bind address.
pub fn resolve_bacnet_bind_address(
    config: &BacnetConfig,
    interfaces: &[NetworkInterface],
) -> Ipv4Addr {
    if let Some(addr) = config.selected_interface {
        return addr;
    }
    if let Some(interface) = interfaces.first() {
        return interface.addr;
    }
    Ipv4Addr::UNSPECIFIED
}

pub fn interface_choices(interfaces: &[NetworkInterface]) -> Vec<Ipv4Addr> {
    let mut choices = interfaces
        .iter()
        .map(|interface| interface.addr)
        .collect::<Vec<_>>();
    choices.sort();
    choices.dedup();
    choices
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BacnetConfig;

    #[test]
    fn interface_choices_deduplicates_addresses() {
        let interfaces = vec![
            NetworkInterface {
                name: "a".to_string(),
                addr: Ipv4Addr::new(10, 0, 0, 1),
            },
            NetworkInterface {
                name: "b".to_string(),
                addr: Ipv4Addr::new(10, 0, 0, 1),
            },
        ];

        assert_eq!(
            interface_choices(&interfaces),
            vec![Ipv4Addr::new(10, 0, 0, 1)]
        );
    }

    #[test]
    fn resolve_bind_prefers_selected_interface() {
        let mut config = BacnetConfig::default();
        config.selected_interface = Some(Ipv4Addr::new(10, 1, 2, 3));
        let interfaces = vec![NetworkInterface {
            name: "en0".to_string(),
            addr: Ipv4Addr::new(192, 168, 1, 5),
        }];
        assert_eq!(
            resolve_bacnet_bind_address(&config, &interfaces),
            Ipv4Addr::new(10, 1, 2, 3)
        );
    }

    #[test]
    fn resolve_bind_falls_back_to_first_discovery_interface() {
        let config = BacnetConfig::default();
        let interfaces = vec![NetworkInterface {
            name: "bridge100".to_string(),
            addr: Ipv4Addr::new(192, 168, 139, 3),
        }];
        assert_eq!(
            resolve_bacnet_bind_address(&config, &interfaces),
            Ipv4Addr::new(192, 168, 139, 3)
        );
    }

    #[test]
    fn excludes_tunnel_and_link_local_interfaces() {
        assert!(!is_bacnet_discovery_interface(
            "utun10",
            Ipv4Addr::new(10, 7, 0, 2)
        ));
        assert!(!is_bacnet_discovery_interface(
            "en0",
            Ipv4Addr::new(169, 254, 1, 1)
        ));
        assert!(is_bacnet_discovery_interface(
            "bridge100",
            Ipv4Addr::new(192, 168, 139, 3)
        ));
        assert!(is_bacnet_discovery_interface(
            "en0",
            Ipv4Addr::new(172, 20, 10, 3)
        ));
    }
}
