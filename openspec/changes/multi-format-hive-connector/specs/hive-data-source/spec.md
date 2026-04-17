## MODIFIED Requirements

### Requirement: Read files in the format declared by HMS InputFormat
The `HiveDataSource` SHALL read files from the storage location specified in HMS table metadata (`sd.location`), selecting the reader implementation by mapping `sd.inputFormat` to a `FileFormat`. Supported mappings cover Parquet, ORC, Avro, JSON (via `TextInputFormat` + JsonSerDe), and CSV/TEXTFILE (via `TextInputFormat` + LazySimpleSerDe/OpenCSVSerde).

#### Scenario: Read non-partitioned Parquet table
- **WHEN** a query scans a non-partitioned Hive table whose `sd.inputFormat` is `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`
- **THEN** the system SHALL list all non-hidden files at `sd.location` and read them using the Parquet reader as a combined RecordBatchStream

#### Scenario: Read non-partitioned ORC table
- **WHEN** a query scans a non-partitioned Hive table whose `sd.inputFormat` is `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`
- **THEN** the system SHALL read its files via the ORC reader and return a RecordBatchStream with the table's projected columns

#### Scenario: Read non-partitioned CSV/TEXTFILE table
- **WHEN** a query scans a Hive table whose `sd.inputFormat` is `org.apache.hadoop.mapred.TextInputFormat` and whose `sd.serdeInfo.serializationLib` is `LazySimpleSerDe` or `OpenCSVSerde`
- **THEN** the system SHALL read its files via the CSV reader configured with delimiter / escape / header options derived from SerDe params

#### Scenario: Read JSON table
- **WHEN** a query scans a Hive table whose `sd.inputFormat` is `TextInputFormat` and whose `sd.serdeInfo.serializationLib` identifies a JSON SerDe (`JsonSerDe` or `org.openx.data.jsonserde.JsonSerDe`)
- **THEN** the system SHALL read its files via the JSON line-delimited reader and project to the HMS-declared schema

#### Scenario: Read Avro table
- **WHEN** a query scans a Hive table whose `sd.inputFormat` is `org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat`
- **THEN** the system SHALL read its files via the Avro reader and return a RecordBatchStream with the table's projected columns

#### Scenario: Read partitioned Hive table (no pruning)
- **WHEN** a query scans a partitioned Hive table
- **THEN** the system SHALL list all partition directories and read files across all partitions using the table-level InputFormat (no filter-based partition pruning in v1)

## ADDED Requirements

### Requirement: Forward HMS SerDe parameters to readers
The `HiveDataSource` SHALL pass the merged map of `sd.serdeInfo.parameters` and table-level parameters to the selected reader, and readers SHALL honour the conventional keys (`field.delim`, `line.delim`, `escape.delim`, `quote.delim`, `skip.header.line.count`, `serialization.null.format`, JSON column mappings) where the underlying reader supports them.

#### Scenario: CSV table with non-default delimiter
- **WHEN** a CSV-format Hive table declares `field.delim = "|"` in its SerDe params
- **THEN** the reader SHALL split records on `|` rather than `,`

#### Scenario: CSV table with header row
- **WHEN** a CSV-format Hive table declares `skip.header.line.count = "1"` in its SerDe params
- **THEN** the reader SHALL skip the first line of each file

### Requirement: Fail loudly on unsupported InputFormat
The `HiveDataSource` SHALL return a clear error when HMS reports an InputFormat that the installed connector does not support. The error SHALL name both the unrecognised InputFormat className and the fully-qualified table name.

#### Scenario: Unknown InputFormat className
- **WHEN** a Hive table's `sd.inputFormat` is a className not present in the supported-format mapping
- **THEN** `HiveDataSource::scan()` SHALL return `ConnectorError::UnsupportedFormat { input_format, table }` identifying the offending className and table

#### Scenario: Recognised InputFormat with unsupported SerDe
- **WHEN** a Hive table uses `TextInputFormat` with a custom, non-registered SerDe className
- **THEN** the system SHALL return the same `UnsupportedFormat` error, naming the unexpected SerDe className, instead of silently reading rows as CSV

### Requirement: Reject mixed-format partitions in v1
The `HiveDataSource` SHALL verify that every partition's `sd.inputFormat` matches the table-level `sd.inputFormat` before scanning. A mismatch SHALL return an error and SHALL NOT attempt a best-effort read.

#### Scenario: Partition overrides table InputFormat
- **WHEN** a partitioned Hive table has at least one partition whose `sd.inputFormat` differs from the table-level InputFormat
- **THEN** scanning the table SHALL return `ConnectorError::UnsupportedFormat` identifying the partition path and both InputFormats

## REMOVED Requirements

### Requirement: Read Parquet data from HMS table location
**Reason**: Superseded by the format-agnostic requirement "Read files in the format declared by HMS InputFormat". Parquet behaviour is preserved as a specific case of the new requirement.
**Migration**: No caller action required — behaviour for Parquet tables is unchanged.

### Requirement: Schema consistency validation
**Reason**: The requirement was Parquet-specific in wording; equivalent schema validation now happens per-format inside each reader (Parquet, ORC, Avro verify against HMS schema at open time; CSV and JSON project by column name with type coercion).
**Migration**: Same semantic guarantee is preserved; callers observing the old error messages should expect format-specific variants going forward.
