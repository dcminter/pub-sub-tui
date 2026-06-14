# Pub-Sub TUI

This is a TUI (Text User Interface) for Google pub/sub, allowing monitoring of the metadata
of a topics on a mock pub/sub instance running locally.

## Features

  * A running count of the number of topics on the current instance
  * A tree-view of topics on the current instance that have non-zero message counts
  * Notes in the tree-view also note
    * Which publishers exist on the topic
      * For each, how many messages they have published
      * Publishers to a topic with a zero-message published count will NOT be shown
    * Which consumers exist on the topic
      * For each, how many messages they have consumed
      * Consumers from a topic with a zero-message consumed count will NOT be shown
  * Basic statistics on how many publishers/consumers in total are connected to the pub/sub instance
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

## How it works

`pub-sub-tui` sits **between** the application under test and the pub/sub server as a
transparent gRPC interception proxy. The application points `PUBSUB_EMULATOR_HOST` at
the TUI, which forwards every call faithfully to the real server (so the application is
unaffected) while observing the traffic in passing. Live traffic gives real publisher,
consumer and message-count information; a one-second admin poll enumerates topics and
subscriptions. See [`docs/architecture.md`](docs/architecture.md) for the full design.

## Getting started

You need Rust (with Cargo) and Docker. No system `protoc` is required — the build
vendors one.

1. **Start a pub/sub emulator** (the mock the tool is built against):

   ```sh
   docker run --rm -p 8085:8085 google/cloud-sdk:emulators \
     gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=test-project
   ```

2. **Run the TUI** (it both proxies and displays):

   ```sh
   cargo run -- --upstream localhost:8085 --listen 127.0.0.1:8681 --project-id test-project
   ```

3. **Point your application under test at the proxy** instead of the emulator:

   ```sh
   export PUBSUB_EMULATOR_HOST=localhost:8681   # the TUI, not the emulator
   ```

   Its topics, subscriptions, publishers, consumers and message counts now appear live
   in the TUI. See [`docs/emulator.md`](docs/emulator.md) for seeding test traffic.

## Usage

```
pub-sub-tui [OPTIONS]

Options:
      --upstream <HOST:PORT>      Real emulator/instance to forward to [default: localhost:8085]
      --listen <HOST:PORT>        Address the proxy listens on            [default: 127.0.0.1:8681]
      --project-id <ID>           Project to enumerate                    [default: test-project]
      --poll-interval-ms <MS>     Admin poll interval                     [default: 1000]
      --log-file <PATH>           Diagnostic log file                     [default: pub-sub-tui.log]
  -h, --help                      Print help
  -V, --version                   Print version
```

Keys: `↑`/`↓` move, `←`/`→` collapse/expand, `Enter` toggle, `q` (or `Ctrl-C`) quit.
The `LOG_LEVEL` environment variable (`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`) sets the
verbosity written to the log file.

## Documentation

  * [`docs/architecture.md`](docs/architecture.md) — design and how each feature maps to real data
  * [`docs/emulator.md`](docs/emulator.md) — running the emulator and seeding test traffic
  * [`docs/development.md`](docs/development.md) — building, testing, linting and the module layout
