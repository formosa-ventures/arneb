# Multi-stage build for arneb benchmark container.
#
# Purpose: run arneb under the same Docker isolation as Trino so the
# TPC-H comparison isn't biased by the native-vs-container overhead
# gap. Throwaway image — not for production distribution.
#
# Build context = repo root. .dockerignore in docker/arneb-bench/
# limits the context to what cargo needs.

FROM rust:1-bookworm AS build
WORKDIR /workspace
COPY . .
# Optional cargo features for the build (e.g. CARGO_FEATURES=heap-profiling
# to compile jemalloc with --enable-prof for a diagnostic image). Empty by
# default → an unchanged production-shaped build.
ARG CARGO_FEATURES=""
# BuildKit cache mounts: persist the cargo registry + target dir across
# builds so an incremental rebuild (only changed crates) takes ~30-60s
# instead of a ~9min cold build. The target dir is a cache mount (not
# baked into the layer), so copy the binary OUT to a real path before
# the next stage can COPY --from it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --bin arneb \
      ${CARGO_FEATURES:+--features "$CARGO_FEATURES"} \
    && cp /workspace/target/release/arneb /workspace/arneb.bin

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/arneb.bin /usr/local/bin/arneb
# Standalone config (legacy single-process bench).
COPY docker/arneb-bench/tpch-hive-container.toml /etc/arneb/config.toml
# Distributed bench configs (coord + 2 workers). Pick via docker
# compose `command:` override per service.
COPY docker/arneb-bench/coord.toml /etc/arneb/coord.toml
COPY docker/arneb-bench/worker-1.toml /etc/arneb/worker-1.toml
COPY docker/arneb-bench/worker-2.toml /etc/arneb/worker-2.toml

# pgwire (clients), Web UI, Flight RPC. Only pgwire is exposed in
# compose by default; the others are reachable from sibling
# containers on the docker network.
EXPOSE 5432 6432 9090

ENTRYPOINT ["/usr/local/bin/arneb"]
CMD ["--config", "/etc/arneb/config.toml"]
