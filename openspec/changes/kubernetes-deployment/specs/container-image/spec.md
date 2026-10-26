## ADDED Requirements

### Requirement: Production container image
The repository SHALL provide `docker/arneb/Dockerfile`, which builds the
`arneb` binary in release mode and runs it as a non-root user on a slim
Debian base with CA certificates. The image SHALL contain no Arneb
configuration and SHALL use `arneb` as its entrypoint.

#### Scenario: Image runs with a mounted config
- **WHEN** the image runs with `--config /etc/arneb/config.toml --role coordinator` and a config mounted at that path
- **THEN** the coordinator starts and accepts pgwire connections

#### Scenario: Non-root
- **WHEN** a container starts from the image
- **THEN** the `arneb` process runs with a non-zero uid

### Requirement: Images are published on every release
When release-please creates a release, CI SHALL build the image for
`linux/amd64` and `linux/arm64` and push one multi-arch manifest to
`ghcr.io/formosa-ventures/arneb` tagged with the full version (`X.Y.Z`),
the minor version (`X.Y`) and `latest`. A manual run with a release tag
SHALL publish that tag the same way.

#### Scenario: Release
- **WHEN** release-please creates release `v0.1.7`
- **THEN** `ghcr.io/formosa-ventures/arneb:0.1.7`, `:0.1` and `:latest` resolve to a manifest with amd64 and arm64 images
