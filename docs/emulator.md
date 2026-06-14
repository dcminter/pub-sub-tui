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

## Run the TUI in front of it

```sh
cargo run -- --upstream localhost:8085 --listen 127.0.0.1:8681 --project-id test-project
```

## Seed test traffic through the proxy

Point any pub/sub client at the **proxy** (`localhost:8681`), not the emulator. Anything
the client does then shows up live in the TUI. A convenient client is `gcloud` itself,
run from the SDK image with host networking:

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

The repository's integration tests drive this exact setup. With an emulator running on
`localhost:8085`:

```sh
PUBSUB_TEST_UPSTREAM=localhost:8085 cargo test --test emulator --test proxy -- --ignored
```

They are `#[ignore]`d by default so the normal `cargo test` needs no emulator.
