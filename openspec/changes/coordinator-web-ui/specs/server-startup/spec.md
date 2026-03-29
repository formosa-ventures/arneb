## MODIFIED Requirements

### Requirement: HTTP server startup for Web UI
The system SHALL start an HTTP server (axum) on a configurable port (default 8080) alongside the existing pgwire and Flight servers. The HTTP server SHALL only be started in coordinator or standalone mode, not in worker-only mode.

#### Scenario: Coordinator starts HTTP server
- **WHEN** the server starts in coordinator mode
- **THEN** the HTTP server starts on the configured HTTP port (default 8080)
- **AND** the log output contains an `info` message with the HTTP listening address

#### Scenario: Worker does not start HTTP server
- **WHEN** the server starts in worker-only mode
- **THEN** no HTTP server is started
- **AND** the pgwire and Flight servers start normally

#### Scenario: Custom HTTP port
- **WHEN** the server starts with `http_port = 9090` in the config
- **THEN** the HTTP server listens on port 9090

### Requirement: HTTP port configuration
The system SHALL add `http_port` to `ServerConfig` with a default value of 8080. The port SHALL follow the same precedence rules as other config values: CLI > env var (`TRINO_HTTP_PORT`) > config file > default.

#### Scenario: Default HTTP port
- **WHEN** no `http_port` is configured
- **THEN** the HTTP server defaults to port 8080

#### Scenario: HTTP port from env var
- **WHEN** `TRINO_HTTP_PORT=9090` is set and no CLI override exists
- **THEN** the HTTP server listens on port 9090

### Requirement: Graceful shutdown includes HTTP server
The system SHALL include the HTTP server in the graceful shutdown sequence. When SIGINT or SIGTERM is received, the HTTP server SHALL stop accepting new connections and finish in-flight requests before shutting down.

#### Scenario: Shutdown stops HTTP server
- **WHEN** the server receives SIGINT while the HTTP server is running
- **THEN** the HTTP server stops accepting new connections
- **AND** the log output contains a shutdown message for the HTTP server

### Requirement: Startup banner includes HTTP port
The startup banner SHALL include the HTTP port when the HTTP server is started.

#### Scenario: Banner with HTTP port
- **WHEN** the server starts in coordinator mode with HTTP on port 8080
- **THEN** the startup banner includes `"HTTP UI on port 8080"` or equivalent
