# Builds the headless monitor and the load generator (the TUI is built too, but it
# is meant to be run on the host, not in the stack). A single image carries all the
# binaries; each compose service just invokes a different one.
#
# No system `protoc` is needed — the build vendors one via `protoc-bin-vendored`.

FROM rust:1-bookworm AS build
WORKDIR /app

# Build the dependency graph first against a stub so that editing the application
# sources does not re-download and re-compile every dependency.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
RUN mkdir -p src/bin \
  && echo 'fn main() {}' > src/bin/pub-sub-monitor.rs \
  && echo 'fn main() {}' > src/bin/pub-sub-tui.rs \
  && echo 'fn main() {}' > src/bin/pub-sub-loadgen.rs \
  && echo '' > src/lib.rs \
  && cargo build --release 2>/dev/null || true

# Now the real sources.
COPY src ./src
# Touch so cargo notices the replaced stub files are newer than the cached build.
RUN touch src/lib.rs src/bin/*.rs && cargo build --release --bins

FROM debian:bookworm-slim AS runtime
# gRPC over TLS is not used (plaintext h2 to the emulator/UI), but ca-certificates
# is cheap insurance if pointed at a real endpoint.
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/pub-sub-monitor /usr/local/bin/pub-sub-monitor
COPY --from=build /app/target/release/pub-sub-loadgen /usr/local/bin/pub-sub-loadgen
COPY --from=build /app/target/release/pub-sub-tui /usr/local/bin/pub-sub-tui

# Default to the headless monitor; compose overrides the command per service.
ENTRYPOINT []
CMD ["pub-sub-monitor"]
