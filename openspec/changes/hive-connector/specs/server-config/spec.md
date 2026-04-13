## MODIFIED Requirements

### Requirement: Multi-catalog configuration
The configuration SHALL support a `[[catalogs]]` array for defining external catalog connections.

#### Scenario: Configure a Hive catalog
- **WHEN** config contains a `[[catalogs]]` entry with `type = "hive"` and `metastore_uri`
- **THEN** the system SHALL register a HiveCatalogProvider with the CatalogManager under the specified name

#### Scenario: Configure multiple catalogs
- **WHEN** config contains multiple `[[catalogs]]` entries with different names
- **THEN** the system SHALL register each as a separate catalog, allowing queries like `SELECT * FROM datalake.analytics.events`

### Requirement: Per-catalog storage credentials
Each catalog entry SHALL support a nested `[catalogs.storage]` section for object store configuration.

#### Scenario: Hive catalog with S3 storage
- **WHEN** a Hive catalog has `[catalogs.storage]` with `type = "s3"` and `region = "us-east-1"`
- **THEN** the system SHALL use these credentials when reading data files for tables in this catalog

#### Scenario: Catalog without storage config
- **WHEN** a Hive catalog has no `[catalogs.storage]` section
- **THEN** the system SHALL fall back to the global `[storage]` configuration or environment variables
