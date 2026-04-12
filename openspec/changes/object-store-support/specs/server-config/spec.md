## MODIFIED Requirements

### Requirement: Table path supports remote URIs
The `[[tables]]` configuration SHALL accept remote URIs (S3, GCS, Azure) in the `path` field in addition to local file paths.

#### Scenario: Configure S3 table
- **WHEN** a table entry has `path = "s3://bucket/data/events.parquet"`
- **THEN** the system SHALL register the table with S3 as the storage backend

#### Scenario: Local path backward compatibility
- **WHEN** a table entry has `path = "/data/local.parquet"` (no URI scheme)
- **THEN** the system SHALL continue to use local filesystem access as before

### Requirement: Global storage configuration
The configuration SHALL support a `[storage.<backend>]` section for global storage backend settings.

#### Scenario: Configure S3 credentials
- **WHEN** config contains `[storage.s3]` with `region` and optional `endpoint`
- **THEN** the system SHALL use these settings when constructing S3 ObjectStore clients. Credentials SHALL be resolved from AWS environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) or IAM role.

#### Scenario: Configure GCS credentials
- **WHEN** config contains `[storage.gcs]` with optional `service_account_path`
- **THEN** the system SHALL use the specified service account file or fall back to Application Default Credentials

#### Scenario: Configure S3-compatible endpoint
- **WHEN** `[storage.s3]` contains `endpoint = "http://localhost:9000"`
- **THEN** the system SHALL connect to the specified endpoint (e.g., MinIO) instead of the default AWS S3 endpoint
