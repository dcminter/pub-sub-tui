# Running the emulator and seeding traffic

The tool is built against the Google Pub/Sub emulator from the
`google/cloud-sdk:emulators` Docker image. Docker is used for all non-Rust tooling here.

## Start the emulator

```sh
docker run --rm -p 8085:8085 google/cloud-sdk:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=test-project
```

- `--host-port=0.0.0.0:8085` makes the emulator reachable from the host.
- `-p 8085:8085` publishes the port.
- The emulator requires no authentication.

Check it is up (it speaks the REST flavour of the API too):

```sh
curl -s http://localhost:8085/v1/projects/test-project/topics
```

> The whole emulator + monitor + loadgen stack can also be brought up at once with
> `docker compose up --build` (see the README); the steps below run the pieces by hand.

## Run the headless monitor in front of it

```sh
cargo run --bin pub-sub-monitor -- \
  --upstream localhost:8085 --listen 127.0.0.1:8681 --state-listen 127.0.0.1:8682
```

Then run the UI (in another terminal), pointed at the monitor's state server:

```sh
cargo run --bin pub-sub-tui -- --monitor 127.0.0.1:8682
```

## Seed test traffic through the proxy

Point any pub/sub client at the **proxy** (`localhost:8681`), not the emulator. Anything
the client does then shows up live in the UI.

### The bundled load generator (easiest)

`pub-sub-loadgen` creates a tree of hierarchically-named topics with publishers and
consumers — ideal for showing off the drill-down topic tree:

```sh
cargo run --bin pub-sub-loadgen -- --endpoint localhost:8681
```

It runs until interrupted (or pass `--duration-secs N`). The topics it creates
(`acme.orders.created`, `acme.billing.invoiced`, `telemetry.device.heartbeat`, …) nest
under `acme`, `telemetry` and `logs` group nodes in the tree.

### Or drive it with `gcloud`

A convenient ad-hoc client is `gcloud` itself, run from the SDK image with host
networking:

```sh
run_gcloud() {
  docker run --rm --network host \
    -e PUBSUB_EMULATOR_HOST=localhost:8681 \
    -e CLOUDSDK_CORE_PROJECT=test-project \
    google/cloud-sdk:latest gcloud "$@"
}

run_gcloud pubsub topics create orders
run_gcloud pubsub subscriptions create billing --topic=orders
run_gcloud pubsub topics publish orders --message="hello"
run_gcloud pubsub subscriptions pull billing --auto-ack
```

After the publish, `orders` appears in the tree with a non-zero message count and a
publisher; after the pull, `billing` shows a consumed count.

> Topics/subscriptions created by talking **directly** to the emulator (port 8085) still
> appear in the tree within ~1s, because the admin poller enumerates them — but their
> publisher/consumer/message activity is only seen when it flows **through** the proxy.

## Automated integration tests

The monitor↔UI boundary is covered by `tests/monitor.rs`, which serves the state stream
and dials it with the UI client over localhost gRPC — no emulator needed, so it runs in
the normal `cargo test`.

The proxy and poller integration tests drive the setup above against a real emulator.
With one running on `localhost:8085`:

```sh
PUBSUB_TEST_UPSTREAM=localhost:8085 cargo test --test emulator --test proxy -- --ignored
```

Those two are `#[ignore]`d by default so the normal `cargo test` needs no emulator.
