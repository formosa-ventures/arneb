## 1. Registry and plumbing

- [x] 1.1 Add `ScalarFunction::invoke(args, num_rows)` (default forwards to `evaluate`) and call it from `expression::evaluate`
- [x] 1.2 Add `FunctionRegistry::register_alias` / `names` and register the Trino alternate spellings
- [x] 1.3 Add `functions/args.rs` normalisation helpers (Utf8 / Int64 / Float64 / Timestamp(us) via Arrow `cast`, arity checks, constant-argument detection)
- [x] 1.4 Switch the existing string functions to the helpers (no panic on `NULL` literals / `LargeUtf8`; `SUBSTRING` accepts `Int32` positions)
- [x] 1.5 Add `chrono` and `regex` as direct dependencies of `arneb-execution`

## 2. Parser lowering

- [x] 2.1 Desugar `IF(cond, a [, b])` to `CASE`
- [x] 2.2 Lower `CEIL(x)` / `FLOOR(x)` special syntax to function calls
- [x] 2.3 Lower `TRIM([BOTH|LEADING|TRAILING] [chars FROM] s)` and `TRIM(s, chars)` to `TRIM`/`LTRIM`/`RTRIM`
- [x] 2.4 Lower `POSITION(a IN b)` to `POSITION(a, b)`
- [x] 2.5 Lower `a || b` to `CONCAT(a, b)`
- [x] 2.6 Parser unit tests for every lowering

## 3. Conditional functions

- [x] 3.1 `GREATEST` / `LEAST` with common-supertype coercion (`variadic_supertype`) and any-NULL → NULL
- [x] 3.2 `TRY` in the expression evaluator with row-level fallback
- [x] 3.3 Unit tests (NULL propagation, mixed numeric types, strings, all-rows-failing TRY)

## 4. String and regex functions

- [x] 4.1 `SPLIT_PART`, `STARTS_WITH`, `STRPOS` (+ instance), `REVERSE`, `LPAD`, `RPAD`
- [x] 4.2 `CHR`, `CODEPOINT`, `CONCAT_WS`, `TRANSLATE`, `LEVENSHTEIN_DISTANCE`, `HAMMING_DISTANCE`
- [x] 4.3 `TRIM`/`LTRIM`/`RTRIM` character-set argument; 2-argument `REPLACE`
- [x] 4.4 `REGEXP_LIKE` (vectorised for constant patterns), `REGEXP_EXTRACT`, `REGEXP_REPLACE` (Java replacement syntax), `REGEXP_COUNT`
- [x] 4.5 Unit tests for each function incl. NULLs, Unicode, and error cases

## 5. Math functions

- [x] 5.1 Unary DOUBLE functions (`SQRT` … `TANH`) via Arrow `unary`
- [x] 5.2 `LOG(b, x)`, `ATAN2` via Arrow `binary`; constants `PI`, `E`, `NAN`, `INFINITY`
- [x] 5.3 `IS_NAN`, `IS_FINITE`, `IS_INFINITE`, `SIGN`, `TRUNCATE`, `RANDOM`
- [x] 5.4 Unit tests (IEEE domain behaviour, integer inputs, NULLs, random ranges)

## 6. Date/time functions

- [x] 6.1 Date-part functions via Arrow `date_part`; `EXTRACT` over DATE and TIMESTAMP with all Trino fields
- [x] 6.2 `DATE_TRUNC` for TIMESTAMP and week/quarter/hour/minute/second units
- [x] 6.3 `DATE_ADD` (month-end clamping), `DATE_DIFF` (Joda month semantics), `LAST_DAY_OF_MONTH`
- [x] 6.4 `NOW`/`CURRENT_TIMESTAMP`, `FROM_UNIXTIME`, `TO_UNIXTIME`, `DATE`
- [x] 6.5 `DATE_FORMAT` / `DATE_PARSE` (MySQL specifiers), `FORMAT_DATETIME` (Joda subset)
- [x] 6.6 Unit tests (ISO weeks, pre-epoch truncation, leap years, formatting/parsing round trips)

## 7. Planner integration

- [x] 7.1 Return types for every new function in `function_return_type`
- [x] 7.2 `planner_and_registry_return_types_agree` test

## 8. End-to-end and docs

- [x] 8.1 `crates/protocol/tests/trino_functions.rs`: SQL → batches through `execute_query` covering SELECT, WHERE, GROUP BY, IF/TRY and special syntax
- [x] 8.2 Rewrite `docs/sql/functions.md` (all 90 functions + "Differences from Trino"); update README, CLAUDE.md, `docs/sql/overview.md`
- [x] 8.3 `cargo fmt -- --check` clean
- [x] 8.4 `cargo clippy --workspace --all-targets -- -D warnings` clean
- [x] 8.5 `cargo test -p arneb-sql-parser -p arneb-planner -p arneb-execution -p arneb-protocol -p arneb-connectors -p arneb-server` green
