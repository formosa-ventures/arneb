## Why

Arneb shipped only 19 scalar functions. Real Trino workloads (dashboards, ETL, ad-hoc analysis) hit missing functions in the first week: `date_add`, `date_diff`, `date_format`, `split_part`, `regexp_like`, `greatest`, `if`, `try`, `year(...)` and friends appear in almost every non-trivial Trino query. Several common *syntaxes* also failed at parse time even though the underlying function existed: `CEIL(x)`, `FLOOR(x)`, `TRIM(BOTH ... FROM s)`, `POSITION(a IN b)` and the `||` operator. Every one of these is a hard "query rejected" for a user migrating from Trino, so closing the most frequently used gap is a go-to-market prerequisite.

## What Changes

- Add 71 Trino functions with Trino semantics (NULL handling, argument order, return types), bringing the total to 90 plus 10 alternate spellings:
  - **Conditional**: `IF` (desugared to `CASE`), `TRY` (per-row error → NULL), `GREATEST`, `LEAST`.
  - **String**: `SPLIT_PART`, `STARTS_WITH`, `STRPOS` (with `instance`), `REVERSE`, `LPAD`, `RPAD`, `CHR`, `CODEPOINT`, `CONCAT_WS`, `TRANSLATE`, `LEVENSHTEIN_DISTANCE`, `HAMMING_DISTANCE`.
  - **Regex**: `REGEXP_LIKE`, `REGEXP_EXTRACT`, `REGEXP_REPLACE`, `REGEXP_COUNT` (Rust `regex`; Java-style `$1` replacements translated).
  - **Math**: `SQRT`, `CBRT`, `EXP`, `LN`, `LOG2`, `LOG10`, `LOG(b, x)`, `SIGN`, `TRUNCATE`, `PI`, `E`, `RANDOM`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `NAN`, `INFINITY`, `IS_NAN`, `IS_FINITE`, `IS_INFINITE`.
  - **Date/time**: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `DAY_OF_WEEK`, `DAY_OF_YEAR`, `YEAR_OF_WEEK`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `DATE_ADD`, `DATE_DIFF`, `LAST_DAY_OF_MONTH`, `NOW`, `FROM_UNIXTIME`, `TO_UNIXTIME`, `DATE`, `DATE_FORMAT`, `DATE_PARSE`, `FORMAT_DATETIME`.
  - **Aliases**: `CEILING`, `POW`, `RAND`, `DAY_OF_MONTH`, `DOW`, `DOY`, `WEEK_OF_YEAR`, `YOW`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`.
- Extend existing functions to Trino semantics: `EXTRACT` gains QUARTER/WEEK/DOW/DOY/YOW/HOUR/MINUTE/SECOND/MILLISECOND and TIMESTAMP input; `DATE_TRUNC` gains TIMESTAMP input and second/minute/hour/week/quarter units; `TRIM`/`LTRIM`/`RTRIM` accept a character set; `REPLACE` accepts the 2-argument form.
- Parser lowering for `CEIL(x)`, `FLOOR(x)`, `TRIM([BOTH|LEADING|TRAILING] [chars FROM] s)`, `TRIM(s, chars)`, `POSITION(a IN b)`, `a || b`, and `IF(...)`.
- Registry: `ScalarFunction::invoke(args, num_rows)` so nullary functions (`now()`, `pi()`, `random()`, `current_date`) produce one value per row (fixes `SELECT current_date FROM t` returning a 1-row column); `FunctionRegistry::register_alias`.
- Argument normalisation helpers so every function accepts NULL literals, `Int32`/`Decimal128` numeric columns and `LargeUtf8` without panicking (previously `UPPER(NULL)` panicked).
- Planner: return types for every new function in `function_return_type`, plus a shared `variadic_supertype` used by both planner and executor for `GREATEST`/`LEAST`.

## Capabilities

### New Capabilities

- `conditional-functions`: `IF`, `TRY`, `GREATEST`, `LEAST`.
- `regexp-functions`: `REGEXP_LIKE`, `REGEXP_EXTRACT`, `REGEXP_REPLACE`, `REGEXP_COUNT`.

### Modified Capabilities

- `string-functions`: new Trino string functions; `TRIM` character sets; 2-arg `REPLACE`.
- `math-functions`: transcendental, trigonometric, classification, `SIGN`, `TRUNCATE`, `RANDOM`, constants.
- `date-functions`: date-part functions, `DATE_ADD`, `DATE_DIFF`, formatting/parsing, unix time, timestamp support in `EXTRACT`/`DATE_TRUNC`.
- `function-registry`: `invoke(args, num_rows)`, aliases, planner/registry return-type agreement.
- `expression-evaluator`: `TRY` evaluation.
- `sql-ast`: lowering of `CEIL`/`FLOOR`/`TRIM`/`POSITION` special syntax, `||`, and `IF`.

## Non-goals

- Session time zones / `TIMESTAMP WITH TIME ZONE` semantics, `AT TIME ZONE`.
- Array/map/JSON/URL/VARBINARY functions (`SPLIT`, `REGEXP_EXTRACT_ALL`, `TO_HEX`, `JSON_EXTRACT`, ...).
- Declared function signatures with planner-side argument coercion.

## Impact

- **Crates**: `sql-parser` (lowering), `planner` (return types, `variadic_supertype`), `execution` (new `functions/{args,conditional,regexp}.rs`, extended `string`/`math`/`date`, `TRY` in `expression.rs`), `protocol` (test-only re-export of `execute_query`).
- **Dependencies**: `chrono` (already in the workspace via `sql-parser`/`protocol`) and `regex` (already in the lockfile via Arrow) become direct dependencies of `arneb-execution`.
- **Performance**: no change to existing hot paths other than `EXTRACT`, which now uses Arrow's vectorised `date_part` kernel instead of a per-row civil-date conversion.
