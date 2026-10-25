# Production image for arneb (published to ghcr.io/formosa-ventures/arneb).
#
# Build context = repo root:
#   docker build -f docker/arneb/Dockerfile -t arneb .
#
# The image carries no config. Mount one and pass it with --config, e.g.
#   docker run -v $PWD/arneb.toml:/etc/arneb/config.toml \
#     ghcr.io/formosa-ventures/arneb --config /etc/arneb/config.toml
# The Web UI bundle is committed under crates/server/frontend, so the build
# needs no Node toolchain.

FROM rust:1-bookworm AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --release --locked --bin arneb \
    && cp /workspace/target/release/arneb /workspace/arneb.bin

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/formosa-ventures/arneb" \
      org.opencontainers.image.description="Arneb distributed SQL query engine" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/arneb --create-home arneb
COPY --from=build /workspace/arneb.bin /usr/local/bin/arneb
USER 10001
WORKDIR /var/lib/arneb

# pgwire, Web UI + /metrics (port + 1000), Trino REST, Flight RPC.
EXPOSE 5432 6432 8080 9090

ENTRYPOINT ["/usr/local/bin/arneb"]
