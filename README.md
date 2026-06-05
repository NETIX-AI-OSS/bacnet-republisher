# BACnet Republisher

Native Rust desktop utility for discovering BACnet/IP devices, polling BACnet point values, and republishing them to a NETIX-compatible MQTT topic tree.

## Features

- Active BACnet/IP discovery with Who-Is/I-Am.
- Device object-list scan where controllers expose `objectList`.
- Manual point entry for constrained controllers.
- MQTT/TLS republishing with scalar payloads compatible with NETIX abstract MQTT ingestion.
- Local TOML configuration with opt-in secret persistence.
- GitHub Actions CI and signed Windows release packaging.

## MQTT contract

Telemetry publishes one scalar value per MQTT topic:

```text
<topic_prefix>/<device_label>/<object_type>_<object_instance>/<property>
```

By default the prefix is `Netix/Site`. The app also publishes health snapshots to the configured health topic, defaulting to `Netix/Site/_health/bacnet-republisher`.

Numeric BACnet values publish as JSON numbers. Boolean, enumerated, and character-string values publish as JSON strings. Failed point reads are omitted and counted in the app status.

## Windows signing

The release workflow expects these GitHub secrets:

- `WINDOWS_CODESIGN_CERT_BASE64`: base64-encoded PFX.
- `WINDOWS_CODESIGN_CERT_PASSWORD`: PFX password.

Optional repository variable:

- `WINDOWS_CODESIGN_TIMESTAMP_URL`: timestamp server URL. Defaults to `http://timestamp.digicert.com`.

Tag a release as `v*` to build, sign, verify, zip, and publish the Windows executable.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo run
```
