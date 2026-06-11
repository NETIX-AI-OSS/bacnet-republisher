# Contributing

Thanks for your interest in contributing! Issues and pull requests are
welcome.

## Development workflow

The CI runs the following checks; please make sure they pass locally
before opening a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

## Pull requests

- Keep changes focused; separate unrelated fixes into their own PRs.
- Add or update tests for behavior changes where practical.
- Note: `vendor/bacnet-transport` is a patched copy of the upstream
  [rusty-bacnet](https://github.com/jscott3201/rusty-bacnet) crate
  (MIT licensed). Prefer fixing issues upstream; only patch the vendored
  copy when a fix is needed before an upstream release.

## License

By contributing, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE).
