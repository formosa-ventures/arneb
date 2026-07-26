# Benchmark runner container.
#
# Purpose: run the tpch-bench harness under the same Docker isolation as the
# engines it measures. This matters for more than tidiness — the DataFusion
# adapter executes *in-process* inside this binary, so as long as the runner is
# a native host process, DataFusion is native no matter what isolation the other
# engines get, and no compose arrangement can make that comparison fair.
#
# Kept separate from docker/arneb-bench/Dockerfile because benchmarks/tpch is
# excluded from the workspace and pulls in the whole DataFusion tree; sharing a
# build would mean recompiling the engine whenever only the harness changes.
#
# Build context = repo root.

FROM rust:1-bookworm AS build
WORKDIR /workspace
COPY . .
# BuildKit cache mounts: persist the cargo registry and target dir across
# builds so an incremental rebuild takes seconds rather than recompiling
# DataFusion. The target dir is a cache mount and is not baked into the layer,
# so the binary is copied out to a real path before the next stage can see it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/benchmarks/tpch/target \
    cd benchmarks/tpch \
    && cargo build --release --bin tpch-bench \
    && cp target/release/tpch-bench /workspace/tpch-bench.bin

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /workspace/tpch-bench.bin /usr/local/bin/tpch-bench
# Queries are baked in so the runner is self-contained; mount over /queries to
# iterate on SQL without rebuilding.
COPY benchmarks/tpch/queries /queries

ENTRYPOINT ["/usr/local/bin/tpch-bench"]
