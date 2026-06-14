# Development

## Prerequisites

- Rust + Cargo (edition 2024; built with rustc 1.95).
- A C/C++ toolchain (used transitively by some crates).
- Docker, for the emulator and any non-Rust tooling.

`protoc` is **not** required on the system: the build vendors a `protoc` binary via the
`protoc-bin-vendored` crate and points the codegen at it (see `build.rs`).

## Build

```sh
cargo build
```

`build.rs` compiles the vendored `google.pubsub.v1` protos (under `proto/`) into both the
server traits the proxy implements and the client stubs it forwards with, using
`tonic-prost-build`.

## Lint and format

The project is run with strict linting — warnings are denied (see the `[lints]` table in
`Cargo.toml`). Generated code is exempted only within `src/pb.rs`.

```sh
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

## Tests

```sh
cargo test                 # unit + render tests; no emulator needed
```

The state-folding logic (`src/observe/state.rs`) and the TUI rendering
(`src/ui/app.rs`, via ratatui's `TestBackend`) are covered by unit tests.

End-to-end tests need a running emulator and are `#[ignore]`d by default:

```sh
# with an emulator on localhost:8085 (see docs/emulator.md)
PUBSUB_TEST_UPSTREAM=localhost:8085 cargo test --test emulator --test proxy -- --ignored
```

- `tests/emulator.rs` — the admin poller lists a seeded topic + subscription.
- `tests/proxy.rs` — traffic driven *through* the proxy is forwarded and observed
  (publish/pull/ack, plus a raw-client `StreamingPull` exercising consumer open/close).

## Module layout

```
build.rs                 proto codegen (vendored protoc; server + client)
proto/                   vendored google.pubsub.v1 + googleapis deps
src/
  lib.rs                 library crate (modules below); main.rs is a thin shell
  cli.rs                 clap derive CLI
  logging.rs             tracing → file, level from LOG_LEVEL
  pb.rs                  generated gRPC bindings (lints allowed here only)
  observe/               Observation events + single-owner state task (mpsc → watch)
  poller.rs              1s admin poll via google-cloud-pubsub (targets upstream directly)
  proxy/                 tonic services: forward.rs (macro), publisher.rs, subscriber.rs
  ui/                    theme, tree, statistics widgets, app view-model, event loop
```

## Dependency notes

- **tonic 0.14** with the split `tonic-prost` / `tonic-prost-build` crates.
- **`google-cloud-pubsub`** is aliased in `Cargo.toml` to the `gcloud-pubsub` crate — the
  current name of the crate formerly published as `google-cloud-pubsub` — so the import
  path and the README's tooling claim stay faithful. `google-cloud-gax` is aliased
  similarly, only to name `Environment::Emulator` for the poller.
- **ratatui 0.30** with **tui-tree-widget 0.24**.
