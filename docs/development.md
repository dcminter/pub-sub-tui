# Development

## Prerequisites

- Rust + Cargo (edition 2024; built with rustc 1.95).
- A C/C++ toolchain (used transitively by some crates).
- Docker, for the emulator and any non-Rust tooling.

`protoc` is **not** required on the system: the build vendors a `protoc` binary via the
`protoc-bin-vendored` crate and points the codegen at it (see `build.rs`).

## Build

```sh
cargo build                  # all three binaries
cargo run --bin pub-sub-monitor -- --help
cargo run --bin pub-sub-tui -- --help
cargo run --bin pub-sub-loadgen -- --help
```

`build.rs` compiles the vendored `google.pubsub.v1` protos and the `monitor.v1` wire
protocol (under `proto/`) into both the server traits the services implement and the
client stubs they dial with, using `tonic-prost-build`.

The crate produces three binaries (`src/bin/`), all thin shells over the library:
`pub-sub-monitor` (headless service), `pub-sub-tui` (UI) and `pub-sub-loadgen` (traffic
generator). A `Dockerfile` and `docker-compose.yml` bring the monitor, the load
generator and an emulator up together; see the README.

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

The state-folding logic (`src/observe/state.rs`), the wire conversion
(`src/monitor/convert.rs`), the TUI rendering and the hierarchical topic tree
(`src/ui/app.rs`, via ratatui's `TestBackend`) are covered by unit tests.
`tests/monitor.rs` covers the monitor↔UI gRPC stream end-to-end over localhost — no
emulator needed, so it runs in the normal suite.

The remaining end-to-end tests need a running emulator and are `#[ignore]`d by default:

```sh
# with an emulator on localhost:8085 (see docs/emulator.md)
PUBSUB_TEST_UPSTREAM=localhost:8085 cargo test --test emulator --test proxy -- --ignored
```

- `tests/emulator.rs` — the admin poller lists a seeded topic + subscription.
- `tests/proxy.rs` — traffic driven *through* the proxy is forwarded and observed
  (publish/pull/ack, plus a raw-client `StreamingPull` exercising consumer open/close).
- `tests/monitor.rs` — state served by the monitor is streamed back to the UI client.

## Module layout

```
build.rs                 proto codegen (vendored protoc; server + client)
proto/                   vendored google.pubsub.v1 + googleapis deps
  monitor/v1/            the monitor↔UI wire protocol (monitor.proto)
src/
  lib.rs                 library crate (modules below); the bins are thin shells
  bin/
    pub-sub-monitor.rs   headless service: proxy + poller + observer + state server
    pub-sub-tui.rs       terminal UI: streams state from a monitor and renders it
    pub-sub-loadgen.rs   demo traffic generator
  cli.rs                 clap derive CLIs (one struct per binary)
  logging.rs             tracing → file (UI) or stderr (headless), level from LOG_LEVEL
  pb.rs                  generated google.pubsub.v1 bindings (lints allowed here only)
  observe/               Observation events + single-owner state task (mpsc → watch)
  monitor/               monitor.v1 gRPC: server, client, AppState↔wire convert, proto
  poller.rs              1s admin poll via google-cloud-pubsub (targets upstream directly)
  proxy/                 tonic services: forward.rs (macro), publisher.rs, subscriber.rs
  loadgen.rs             traffic generator (raw pb gRPC clients through the proxy)
  ui/                    theme, hierarchical tree, statistics widgets, app view-model, loop
```

## Dependency notes

- **tonic 0.14** with the split `tonic-prost` / `tonic-prost-build` crates.
- **`google-cloud-pubsub`** is aliased in `Cargo.toml` to the `gcloud-pubsub` crate — the
  current name of the crate formerly published as `google-cloud-pubsub` — so the import
  path and the README's tooling claim stay faithful. `google-cloud-gax` is aliased
  similarly, only to name `Environment::Emulator` for the poller.
- **ratatui 0.30** with **tui-tree-widget 0.24**.
