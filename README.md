# BACnet Republisher

Native Rust desktop utility for discovering BACnet/IP devices, polling BACnet point values, and republishing them to a NETIX-compatible MQTT topic tree.

## Features

- Active BACnet/IP discovery with Who-Is/I-Am.
- Device object-list scan where controllers expose `objectList`, with best-effort metadata preview.
- Manual point entry for constrained controllers.
- MQTT/TLS republishing with scalar payloads compatible with NETIX abstract MQTT ingestion.
- Optional PEM-based MQTT TLS CA, client certificate, and client key paths.
- Per-point stale/read/publish status in the app.
- Local TOML configuration with opt-in secret persistence.
- GitHub Actions CI and signed Windows release packaging.

## MQTT contract

Telemetry publishes one scalar value per MQTT topic:

```text
<topic_prefix>/<device_label>/<object_type>_<object_instance>/<property>
```

By default the prefix is `Netix/Site`. The app also publishes health snapshots to the configured health topic, defaulting to `Netix/Site/_health/bacnet-republisher`.

Numeric BACnet values publish as JSON numbers. Boolean, enumerated, and character-string values publish as JSON strings. Failed point reads are omitted and counted in the app status.

MQTT TLS uses platform root certificates by default. Operators can optionally configure CA, client certificate, and client key PEM file paths in Settings. Passwords and client key passphrases are only written to the TOML config when `Remember secrets` is enabled.

## Windows signing

The release workflow expects these GitHub secrets:

- `WINDOWS_CODESIGN_CERT_BASE64`: base64-encoded PFX.
- `WINDOWS_CODESIGN_CERT_PASSWORD`: PFX password.

Optional repository variable:

- `WINDOWS_CODESIGN_TIMESTAMP_URL`: timestamp server URL. Defaults to `http://timestamp.digicert.com`.

Tag a release as `v*` to build, sign, verify, zip, and publish the Windows executable.

Release assets are the platform zip files (`bacnet-republisher-<tag>-windows-x86_64.zip` and `...-linux-x86_64.zip`), not GitHub's auto-generated "Source code" archives. Extract the zip and run `bacnet-republisher.exe` (Windows) or `bacnet-republisher` (Linux). Windows builds statically link the MSVC runtime, so the Visual C++ Redistributable is not required.

## Configuration

Copy [config.example.toml](config.example.toml) to `config.toml` next to the executable and edit it there, or configure everything from Settings in the app. The local `config.toml` is gitignored.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked --release
cargo run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

`vendor/bacnet-transport` contains a patched copy of the `bacnet-transport` crate from the [rusty-bacnet](https://github.com/jscott3201/rusty-bacnet) project, licensed under the [MIT License](vendor/bacnet-transport/LICENSE). See [NOTICE](NOTICE) for details.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project by you shall be licensed under the Apache License 2.0, without any additional terms or conditions.
