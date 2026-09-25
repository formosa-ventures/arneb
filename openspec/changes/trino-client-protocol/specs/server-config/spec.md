## ADDED Requirements

### Requirement: Trino client protocol listener configuration
The system SHALL read a `[trino]` section with `enabled` (default `true`) and `port` (default `8080`), overridable by `ARNEB_TRINO_ENABLED` / `ARNEB_TRINO_PORT` and by the CLI flags `--no-trino` / `--trino-port`, with precedence CLI > env > file > default. The listener SHALL bind `bind_address:port` on the coordinator and standalone roles only, SHALL log the effective settings on the `arneb::config` target, and a bind failure SHALL be logged without stopping the server.

#### Scenario: Defaults
- **WHEN** the config file has no `[trino]` section
- **THEN** the Trino listener is enabled on port 8080

#### Scenario: File settings
- **WHEN** the config file contains `[trino]` with `enabled = false` and `port = 18080`
- **THEN** the parsed config has the listener disabled with port 18080

#### Scenario: Port already in use
- **WHEN** another process already listens on the configured Trino port
- **THEN** the server logs an error and continues serving pgwire and the Web UI

#### Scenario: Worker role
- **WHEN** the server runs with `--role worker`
- **THEN** no Trino listener is started
