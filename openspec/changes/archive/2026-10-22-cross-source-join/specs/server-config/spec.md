## MODIFIED Requirements

### Requirement: PostgreSQL catalog entries
`[[catalogs]]` entries SHALL accept `type = "postgresql"` alongside `"hive"`.
A `postgresql` entry SHALL accept `connection_url`, `user`, `password`,
`max_connections`, `split_min_rows`, `max_splits`,
`unconstrained_numeric_scale`, `domain_compaction_threshold` and
`default_schema`. `domain_compaction_threshold` SHALL default to 1000 and SHALL
be at least 1. Hive-only fields (`metastore_uri`, `storage`) SHALL be ignored on
a `postgresql` entry, and PostgreSQL-only fields SHALL be ignored on a `hive`
entry. The effective non-secret settings of each mounted PostgreSQL catalog
SHALL be logged at startup. An unknown catalog `type` SHALL keep today's
behavior: logged and skipped.

#### Scenario: Mixed catalogs
- **WHEN** the config has one `hive` catalog and one `postgresql` catalog
- **THEN** both are mounted and both can be referenced in the same query

#### Scenario: Effective settings logged
- **WHEN** a `postgresql` catalog is mounted
- **THEN** a startup log line shows its name, host, database, `max_connections`, `split_min_rows`, `max_splits` and `domain_compaction_threshold`, without the password

#### Scenario: Invalid compaction threshold
- **WHEN** a `postgresql` entry sets `domain_compaction_threshold = 0`
- **THEN** startup fails with an error naming the catalog and the field
