## ADDED Requirements

### Requirement: Plan scans from manifests
The system SHALL find the data files of the pinned snapshot by reading its manifest list and manifests (Avro object container files). For legacy v1 snapshots, it SHALL read the inline `manifests` list instead.

#### Scenario: Live files only
- **WHEN** a manifest contains entries with status `ADDED`, `EXISTING`, and `DELETED`
- **THEN** the scan SHALL include the `ADDED` and `EXISTING` data files and SHALL NOT open any `DELETED` (tombstoned) file

#### Scenario: Provably empty manifests
- **WHEN** a manifest-list entry records zero added and zero existing files
- **THEN** the system SHALL NOT fetch that manifest

### Requirement: Reject row-level delete files
The system SHALL NOT return rows from a snapshot that has live delete files, because it doesn't apply them.

#### Scenario: Position deletes present
- **WHEN** the pinned snapshot has a delete manifest with a live position delete file (for example, after a merge-on-read `DELETE`)
- **THEN** the query SHALL fail with an `unsupported operation` error that names the table, the snapshot, and the delete-file kind, and that suggests compacting the table

#### Scenario: Deletes compacted away
- **WHEN** the table is compacted (for example, with Trino `ALTER TABLE … EXECUTE optimize`) and the new current snapshot has no live delete files
- **THEN** the table SHALL be readable, and its results SHALL match other engines

### Requirement: Reject unsupported data file formats
The system SHALL read only Parquet data files.

#### Scenario: ORC data file
- **WHEN** a live data file has `file_format = ORC` or `AVRO`
- **THEN** the query SHALL fail with an `unsupported operation` error that names the file

### Requirement: Resolve columns by field ID
The system SHALL match table columns to data-file columns by Iceberg field ID (the Parquet `field_id`), not by name or position.

#### Scenario: Renamed column
- **WHEN** column 2 was renamed from `n_name` to `nation_name` after some files were written
- **THEN** `SELECT nation_name` SHALL return the values from those older files

#### Scenario: Added column
- **WHEN** a column was added after a data file was written
- **THEN** rows from that file SHALL return `NULL` for the column

#### Scenario: Promoted type
- **WHEN** a column was promoted from `int` to `long`
- **THEN** values from files that store it as `int` SHALL be returned as Int64

#### Scenario: Reordered physical columns
- **WHEN** a data file stores the table's columns in a different physical order
- **THEN** each output column SHALL contain the values of the matching field ID

#### Scenario: Files without field IDs
- **WHEN** a data file has no Parquet field IDs
- **THEN** the system SHALL match columns with the `schema.name-mapping.default` table property, or with current column names if that property isn't set

### Requirement: Pushdown into data files
The system SHALL apply projection pushdown, row-group statistics pruning, and row-filter predicate pushdown to each data file. Filter column references SHALL be translated to that file's physical columns by field ID.

#### Scenario: Type-safe filter pushdown
- **WHEN** a pushed filter references a column whose physical type in a file differs from the table type, or compares it to a literal of a different type
- **THEN** that filter SHALL NOT be pushed into that file's Parquet reader. The engine SHALL still evaluate it above the scan

### Requirement: File pruning from manifest metadata
Before opening a data file, the system SHALL skip the file when its manifest column bounds, or its identity-partition values, prove that no row can satisfy the pushed conjunctive filters.

#### Scenario: Identity partition pruning
- **WHEN** `ice.orders` is identity-partitioned on `o_orderpriority` and the filter is `o_orderpriority = '1-URGENT'`
- **THEN** the files of the other four partitions SHALL NOT be opened

#### Scenario: Bounds pruning
- **WHEN** a file's `o_orderdate` bounds are `[1995-06-01, 1995-06-30]` and the filter is `o_orderdate = DATE '1998-08-02'`
- **THEN** that file SHALL NOT be opened

#### Scenario: Conservative evaluation
- **WHEN** a filter uses `OR`, a cast, a floating-point column, or a literal that can't be represented exactly in the column's type
- **THEN** the filter SHALL NOT be used to prune files
