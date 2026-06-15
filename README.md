# Pub-Sub TUI

Monitor a Google Pub/Sub instance (typically the local emulator) by transparently
proxying and observing its gRPC traffic. The tool is split into two parts:

- **`pub-sub-monitor`** — a **headless service**: a transparent gRPC interception
  proxy plus an admin poller. It observes traffic and exposes the live state over a
  small gRPC API. It runs anywhere your Pub/Sub instance runs — for example inside a
  `docker-compose` stack.
- **`pub-sub-tui`** — the **terminal UI**: it connects to a (possibly remote)
  `pub-sub-monitor` and displays what it sees. It is meant to be run from *outside*
  the stack, on your own machine.

A third binary, **`pub-sub-loadgen`**, generates demo traffic (a tree of
hierarchically-named topics, with publishers and consumers) so there is something to
look at.

## Features

  * A running count of the number of topics on the current instance
  * A tree-view of topics on the current instance that have non-zero message counts
  * Topic names form a **drill-down hierarchy**: a dotted name like
    `acme.orders.created` nests under `acme ▸ orders ▸ created`, so related topics
    can be collapsed and expanded as a tree
  * Notes in the tree-view also note
    * Which publishers exist on the topic
      * For each, how many messages they have published
      * Publishers to a topic with a zero-message published count will NOT be shown
    * Which consumers exist on the topic
      * For each, how many messages they have consumed
      * Consumers from a topic with a zero-message consumed count will NOT be shown
  * Basic statistics on how many publishers/consumers in total are connected to the pub/sub instance
  * A live feed of recently-published messages along the bottom of the screen. `Tab` into
    it to pause the auto-scroll and cursor through history, then `Enter` to inspect a
    message — shown as pretty-printed, syntax-highlighted JSON, plain text, or a hex dump
  * A connection indicator in the title bar shows whether the UI is currently connected to
    the monitor, so an empty view reads as "not connected" rather than "no traffic"
  * The TUI handles re-sizes of the terminal window automatically
  * The TUI colour scheme apes the old-skool Borland IDE style!
  * The tool only reads metadata - applications running against the same pub/sub instance will be
    completely unaffected.
  * The content of the tool is updated in real-time (to the extent possible) or on a one-second
    tick where polling is required.
  * A `LOG_LEVEL` environment variable is honoured at TRACE, DEBUG, INFO, WARN, and ERROR levels.

## Tooling

  * The pub/sub mock to be used is `google/cloud-sdk:emulators` (a Docker image)
  * The ratatui crate (along with crossterm) is used for UI rendering
  * The clap crate is used for command-line rendering
    * Declarative mode is used for the Clap tool - all clap config is therefore in the Rust source
  * The google-cloud-pubsub crate is used for pub/sub metadata access
  * The monitor↔UI boundary is a `tonic` gRPC service (`proto/monitor/v1/monitor.proto`)

## How it works

`pub-sub-monitor` sits **between** the application under test and the Pub/Sub server
as a transparent gRPC interception proxy. The application points
`PUBSUB_EMULATOR_HOST` at the monitor, which forwards every call faithfully to the
real server (so the application is unaffected) while observing the traffic in
passing. Live traffic gives real publisher, consumer and message-count information; a
one-second admin poll enumerates topics and subscriptions.

The monitor folds everything it sees into an in-memory state and streams immutable
snapshots of it over the `monitor.v1.Monitor` gRPC service. One or more `pub-sub-tui`
UIs connect to that service and render the snapshots. See
[`docs/architecture.md`](docs/architecture.md) for the full design.

## Quick start — the docker-compose demo

This brings up the emulator, the monitor and the load generator together, and you
run the UI on your host against the stack. You need Docker and Rust (for the UI).

1. **Start the stack** (emulator + monitor + loadgen):

   ```sh
   docker compose up --build
   ```

   The monitor publishes two ports to your host: `8681` (the proxy) and `8682` (the
   state stream the UI consumes). The load generator immediately starts creating a
   tree of topics and pushing traffic through the proxy.

2. **Run the UI on your host**, pointing it at the monitor's state port:

   ```sh
   cargo run --bin pub-sub-tui -- --monitor 127.0.0.1:8682
   ```

   The hierarchical topic tree, publishers, consumers and message counts appear and
   update live. Use `←`/`→` to collapse/expand the `acme`, `telemetry` and `logs`
   branches.

3. **Tear it down** with `docker compose down` (and `q` to quit the UI).

## Manual start (no compose)

Run each piece yourself — useful when monitoring your *own* application's traffic
rather than the load generator's.

1. **Start an emulator:**

   ```sh
   docker run --rm -p 8085:8085 google/cloud-sdk:emulators \
     gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=test-project
   ```

2. **Start the headless monitor** in front of it:

   ```sh
   cargo run --bin pub-sub-monitor -- \
     --upstream localhost:8085 --listen 127.0.0.1:8681 --state-listen 127.0.0.1:8682
   ```

3. **Run the UI:**

   ```sh
   cargo run --bin pub-sub-tui -- --monitor 127.0.0.1:8682
   ```

4. **Point an application (or the load generator) at the proxy** instead of the
   emulator, so its traffic is observed:

   ```sh
   export PUBSUB_EMULATOR_HOST=localhost:8681   # the monitor's proxy, not the emulator
   # ...or generate demo traffic:
   cargo run --bin pub-sub-loadgen -- --endpoint localhost:8681
   ```

   See [`docs/emulator.md`](docs/emulator.md) for more on seeding traffic.

## Usage

```
pub-sub-monitor [OPTIONS]      # headless: proxy + poller + state server
      --upstream <HOST:PORT>       Real emulator/instance to forward to   [default: localhost:8085]
      --listen <HOST:PORT>         Address the proxy listens on           [default: 0.0.0.0:8681]
      --state-listen <HOST:PORT>   Address the state server listens on    [default: 0.0.0.0:8682]
      --project-id <ID>            Project to enumerate                   [default: test-project]
      --poll-interval-ms <MS>      Admin poll interval                    [default: 1000]
      --recent-buffer <N>          Recent messages kept for the panel     [default: 200]
      --max-payload-bytes <BYTES>  Per-message bytes captured (truncates)  [default: 65536]

pub-sub-tui [OPTIONS]          # the terminal UI
      --monitor <HOST:PORT>        Monitor state server to connect to     [default: 127.0.0.1:8682]
      --log-file <PATH>            Diagnostic log file                    [default: pub-sub-tui.log]

pub-sub-loadgen [OPTIONS]      # demo traffic generator
      --endpoint <HOST:PORT>       Pub/Sub endpoint (point at the proxy)  [default: localhost:8681]
      --project-id <ID>            Project to create topics under         [default: test-project]
      --interval-ms <MS>           Delay between publish rounds           [default: 800]
      --duration-secs <S>          Run for S seconds (0 = forever)        [default: 0]
```

Keys: `Tab` switches focus between the topic tree and the messages panel. In the
tree, `↑`/`↓` move, `←`/`→` collapse/expand and `Enter` toggles. In the messages
panel, `↑`/`↓` select (pausing the live scroll), `Enter` opens a message and `Esc`
returns. `q` (or `Ctrl-C`) quits from anywhere.
The `LOG_LEVEL` environment variable (`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`) sets the
verbosity — written to a file for the UI (which owns the terminal) and to stderr for
the headless binaries.

## Documentation

  * [`docs/architecture.md`](docs/architecture.md) — design and how each feature maps to real data
  * [`docs/emulator.md`](docs/emulator.md) — running the emulator and seeding test traffic
  * [`docs/development.md`](docs/development.md) — building, testing, linting and the module layout
