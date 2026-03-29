## ADDED Requirements

### Requirement: Data generation script
The system SHALL provide a shell script `scripts/generate_data.sh` that wraps the official TPC-H `dbgen` tool to generate data at configurable scale factors (SF1, SF10, SF100). The script SHALL accept the scale factor as a command-line argument and output CSV files to `data/sf{N}/csv/`.

#### Scenario: Generate SF1 data
- **WHEN** `./scripts/generate_data.sh 1` is executed
- **THEN** CSV files for all 8 TPC-H tables are generated in `data/sf1/csv/`
- **AND** each file contains the correct number of rows for SF1

#### Scenario: Generate SF10 data
- **WHEN** `./scripts/generate_data.sh 10` is executed
- **THEN** CSV files are generated in `data/sf10/csv/` with approximately 10x the rows of SF1

#### Scenario: Missing dbgen
- **WHEN** `dbgen` is not found on PATH
- **THEN** the script prints an error message with build instructions and exits with non-zero code

### Requirement: CSV to Parquet conversion
The system SHALL provide a Rust helper binary that converts TPC-H CSV files to Parquet format with correct Arrow schemas. The converter SHALL read from `data/sf{N}/csv/` and write to `data/sf{N}/parquet/`.

#### Scenario: Convert lineitem CSV to Parquet
- **WHEN** the converter processes `data/sf1/csv/lineitem.tbl`
- **THEN** it produces `data/sf1/parquet/lineitem.parquet` with the correct schema (16 columns)

#### Scenario: All 8 tables converted
- **WHEN** the converter runs for SF1
- **THEN** Parquet files are produced for: lineitem, orders, customer, part, partsupp, supplier, nation, region

### Requirement: TPC-H table schemas
The system SHALL define Arrow schemas for all 8 TPC-H tables matching the official TPC-H specification. Schemas SHALL use appropriate Arrow types: Int32/Int64 for keys, Float64 for monetary values, Utf8 for strings, Date32 for dates.

#### Scenario: Lineitem schema
- **WHEN** the lineitem schema is loaded
- **THEN** it contains 16 columns: l_orderkey (Int64), l_partkey (Int64), l_suppkey (Int64), l_linenumber (Int32), l_quantity (Float64), l_extendedprice (Float64), l_discount (Float64), l_tax (Float64), l_returnflag (Utf8), l_linestatus (Utf8), l_shipdate (Date32), l_commitdate (Date32), l_receiptdate (Date32), l_shipinstruct (Utf8), l_shipmode (Utf8), l_comment (Utf8)

#### Scenario: Nation schema
- **WHEN** the nation schema is loaded
- **THEN** it contains 4 columns: n_nationkey (Int32), n_name (Utf8), n_regionkey (Int32), n_comment (Utf8)
