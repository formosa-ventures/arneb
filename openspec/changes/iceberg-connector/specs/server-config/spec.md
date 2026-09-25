## MODIFIED Requirements

### Requirement: Multi-catalog configuration
The configuration SHALL support a `[[catalogs]]` array for defining external catalog connections. `type` SHALL accept `"hive"` and `"iceberg"`.

#### Scenario: Configure a Hive catalog
- **WHEN** config contains a `[[catalogs]]` entry with `type = "hive"` and `metastore_uri`
- **THEN** the system SHALL register a HiveCatalogProvider and a HiveConnectorFactory under the specified name

#### Scenario: Configure an Iceberg catalog
- **WHEN** config contains a `[[catalogs]]` entry with `type = "iceberg"` and `metastore_uri`
- **THEN** the system SHALL register an IcebergCatalogProvider and an IcebergConnectorFactory under the specified name, using the catalog's merged storage configuration

#### Scenario: Hive and Iceberg catalogs on one metastore
- **WHEN** config contains a `hive` catalog and an `iceberg` catalog with the same `metastore_uri`
- **THEN** each catalog SHALL serve only its own table kind. Querying the other kind SHALL fail with an error that names the right catalog type

#### Scenario: Unknown catalog type
- **WHEN** a `[[catalogs]]` entry has a `type` other than `"hive"` or `"iceberg"`
- **THEN** the system SHALL log a warning and skip the entry

## ADDED Requirements

### Requirement: Surface data source creation errors
If a connector fails to create a data source for a scanned table, the query SHALL fail with that connector error. This applies both on the coordinator and standalone path and in worker tasks.

#### Scenario: Connector rejects a table
- **WHEN** a connector's `create_data_source` returns an error (for example, an Iceberg table with delete files)
- **THEN** the client SHALL receive that error message, not a generic "data source not found" error
