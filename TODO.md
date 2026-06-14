# pub-sub-tui — implementation TODO

Living checklist that mirrors the agreed plan. Updated as work progresses.

## Goal

Make every claim in `README.md` true **without changing the claims**, by building the
TUI it describes. Design: an **interception gRPC proxy** that sits between the
client-under-test and the Pub/Sub emulator, forwarding all traffic faithfully (the
client is unaffected) while observing it. This yields *real* data for the claims that
Pub/Sub metadata alone cannot provide (publishers, per-entity message counts).

How README claims map to the design:
- **Topic count / topic tree / consumers exist** → 1s admin poll (`google-cloud-pubsub`).
- **Publishers + per-publisher counts** → observed `Publish` traffic, keyed by peer.
- **Consumers + consumed counts** → observed `Pull`/`StreamingPull`/`Acknowledge`.
- **Non-zero-message-count tree filter** → observed publish counts.
- **Real-time vs 1s polling tick** → live traffic stream vs admin poll.
- **Borland theme / resize / metadata-only / LOG_LEVEL** → TUI + tracing-to-file.

## Phase 0 — build plumbing + proto codegen  ✅ DONE
- [x] `Cargo.toml` deps (tonic 0.14, prost 0.14, ratatui 0.29, tui-tree-widget 0.24, clap, tracing…)
- [x] Vendor `google/pubsub/v1` proto closure under `proto/`
- [x] `build.rs` codegen via `protoc-bin-vendored` (no system protoc needed); server + client
- [x] `src/pb.rs` (generated include, lints scoped-allowed)
- [x] `src/cli.rs` (clap derive), `src/logging.rs` (LOG_LEVEL → file)
- [x] Builds clean; `cargo clippy --all-targets` green; `LOG_LEVEL` honoured

## Phase 1 — admin-poll-only TUI  ✅ DONE
- [x] `src/observe/` — `Observation` events, `ObservationSink`, `AppState`, state task (mpsc→watch)
- [x] `src/poller.rs` — list topics + subscriptions every 1s against **real upstream** → AdminSnapshot
      (uses `google-cloud-pubsub` crate, per README; targets upstream via `Environment::Emulator`)
- [x] `src/ui/` — theme (Borland blue/yellow), tree (tui-tree-widget), app view-model, render loop
- [x] `src/main.rs` (+ `src/lib.rs`) — `#[tokio::main]`, spawn observer + poller, run TUI; resize auto
- [x] Tests: state-fold unit tests, TestBackend render tests, **emulator integration test (passing)**
- [x] Verified: poller lists seeded topic+subscription against the real `google/cloud-sdk:emulators`

## Phase 2 — proxy unary RPCs  ✅ DONE
- [x] `src/proxy/` — upstream `Channel` (lazy); implement `Publisher` + `Subscriber` traits
- [x] Faithful forwarding for all RPCs (`proxy_service!` macro emits the `#[async_trait]` impl)
- [x] Taps: `Publish` (per-topic + per-publisher counts), `Pull`/`Acknowledge` (consumed counts)
- [x] Peer-address identity for publishers
- [x] Serve proxy on `--listen`; **verified** via integration test (client through proxy → counts)

## Phase 3 — StreamingPull bidi proxy  ✅ DONE
- [x] Active bidi pump (`tokio::select!`) — avoids the establish-before-feed deadlock
- [x] Latch subscription from first frame; count acks (client→server) and deliveries (server→client)
- [x] Live-consumer open/close tracking; close detected promptly on either side ending
- [x] **Verified** via raw-client integration test (open → deliver → ack → close → 0)

## Phase 4 — polish + docs + verification  ✅ DONE
- [x] Non-zero-count tree filter; sliding-window active publishers; totals panel
- [x] Upstream reconnect handling (lazy channel); terminal teardown via ratatui panic hook
- [x] Ctrl-C handling in the TUI
- [x] README: added Getting Started / Usage (claims unchanged); `docs/architecture.md`,
      `docs/emulator.md`, `docs/development.md`
- [x] End-to-end verification against `google/cloud-sdk:emulators` (publish/pull/stream through proxy)
- [x] Final `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check` + full test run
- [x] Real-binary smoke test under a pty (proxy + poller start cleanly, no panics)

## Status: complete

Every README feature and tooling claim is implemented and backed by real data, verified by
unit tests, render tests, and emulator integration tests. Run the app per the README; run
`cargo test` (no emulator) or the `--ignored` integration tests (with an emulator) to verify.

## Notable decisions / deviations from the original plan
- **tonic 0.14** (not 0.13); codegen split into `tonic-prost-build` + `tonic-prost`.
- **No system `protoc`/`cmake`** available → use `protoc-bin-vendored` (prebuilt binary +
  bundled well-known includes) instead of `protobuf-src`.
- The `google-cloud-pubsub` crate was renamed to `gcloud-pubsub` (1.x) and a *new official*
  `google-cloud-pubsub` 1.1.0 also exists. Crate choice for the poller pending research
  (must support an explicit upstream endpoint, since the env var points at the proxy).
- Logs go to a **file** (`--log-file`, default `pub-sub-tui.log`), not stderr, so they don't
  corrupt the TUI; `LOG_LEVEL` controls verbosity (case-insensitive).
- "Connected publishers" = peers active within a 10s window (unary publish has no socket).
