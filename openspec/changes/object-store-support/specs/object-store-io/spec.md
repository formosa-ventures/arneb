## ADDED Requirements

### Requirement: Unified storage abstraction
The system SHALL provide a `StorageRegistry` that manages `ObjectStore` instances for local filesystem, AWS S3, GCP GCS, and Azure Blob Storage through the Apache `object_store` crate.

#### Scenario: Local filesystem access
- **WHEN** a path has no URI scheme or uses `file://`
- **THEN** the system SHALL use `LocalFileSystem` as the ObjectStore implementation

#### Scenario: S3 access
- **WHEN** a path starts with `s3://` or `s3a://`
- **THEN** the system SHALL use `AmazonS3` as the ObjectStore implementation, configured with credentials from environment variables or the global `[storage.s3]` config

#### Scenario: GCS access
- **WHEN** a path starts with `gs://`
- **THEN** the system SHALL use `GoogleCloudStorage` as the ObjectStore implementation, configured with Application Default Credentials or the global `[storage.gcs]` config

#### Scenario: Azure access
- **WHEN** a path starts with `abfss://` or `az://`
- **THEN** the system SHALL use `MicrosoftAzure` as the ObjectStore implementation, configured with credentials from the global `[storage.azure]` config

### Requirement: ObjectStore instance caching
The `StorageRegistry` SHALL cache ObjectStore instances by scheme and bucket/container to avoid redundant client construction.

#### Scenario: Multiple tables in same S3 bucket
- **WHEN** two tables reference paths in the same S3 bucket (e.g., `s3://bucket/a.parquet` and `s3://bucket/b.parquet`)
- **THEN** the system SHALL reuse the same `AmazonS3` ObjectStore instance for both

### Requirement: URI parsing
The system SHALL parse storage URIs into a scheme, bucket/container, and object path.

#### Scenario: Parse S3 URI
- **WHEN** given URI `s3://my-bucket/path/to/file.parquet`
- **THEN** the system SHALL extract scheme `s3`, bucket `my-bucket`, and path `path/to/file.parquet`

#### Scenario: Parse plain local path
- **WHEN** given path `/data/file.parquet` (no scheme)
- **THEN** the system SHALL treat it as a local filesystem path
