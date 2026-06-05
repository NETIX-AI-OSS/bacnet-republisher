use crate::model::NetworkInterface;
use get_if_addrs::{get_if_addrs, IfAddr};
use std::net::Ipv4Addr;

pub fn ipv4_interfaces() -> Vec<NetworkInterface> {
    let mut interfaces = get_if_addrs()
        .map(|interfaces| {
            interfaces
                .into_iter()
                .filter_map(|interface| match interface.addr {
                    IfAddr::V4(v4) if !v4.ip.is_loopback() && !v4.ip.is_unspecified() => {
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
}
