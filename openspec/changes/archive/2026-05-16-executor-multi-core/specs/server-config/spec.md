## ADDED Requirements

### Requirement: [execution] config block

The system SHALL accept an `[execution]` table in `arneb.toml` with the following keys:

```toml
[execution]
target_partitions = 14            # default: num_cpus::get()
parallel_file_scan = true         # default: true
parallel_hash_aggregate = true    # default: true
parallel_aggregate_min_groups = 1024  # default: 1024
channel_capacity = 4              # default: 4 (mpsc capacity per partition)
```

When omitted, defaults apply. Invalid values (e.g. `target_partitions = 0`) SHALL produce a clear startup error before serving traffic.

#### Scenario: Default target_partitions equals CPU count

- **GIVEN** `arneb.toml` has no `[execution]` block
- **WHEN** the server starts on a 14-core machine
- **THEN** `target_partitions` resolves to `14` (from `num_cpus::get()`)

#### Scenario: Override via toml

- **GIVEN** `arneb.toml` containing `[execution] target_partitions = 4`
- **WHEN** the server starts
- **THEN** `target_partitions` resolves to `4`

#### Scenario: Invalid value rejected

- **GIVEN** `arneb.toml` containing `[execution] target_partitions = 0`
- **WHEN** the server starts
- **THEN** startup fails with a clear error citing the invalid key

### Requirement: --target-partitions CLI override

The system SHALL accept a `--target-partitions=N` CLI flag on the `arneb` binary that overrides `[execution] target_partitions` from `arneb.toml`. Precedence (highest first): CLI > env (`ARNEB_TARGET_PARTITIONS`) > file > default.

#### Scenario: CLI overrides file

- **GIVEN** `arneb.toml` has `target_partitions = 4` and CLI flag `--target-partitions=8`
- **WHEN** the server starts
- **THEN** `target_partitions` resolves to `8`
