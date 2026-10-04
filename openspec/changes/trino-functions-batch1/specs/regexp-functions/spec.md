## ADDED Requirements

### Requirement: Regular expression dialect
Regular-expression functions SHALL compile patterns with the Rust `regex` crate. Invalid patterns (including unsupported look-around and back-references) SHALL produce `ExecutionError::InvalidOperation` naming the pattern. Compiled patterns SHALL be reused while consecutive rows share the same pattern.

#### Scenario: Invalid pattern
- **WHEN** `REGEXP_LIKE('a', '(')` is evaluated
- **THEN** it returns an error mentioning the invalid regular expression

### Requirement: REGEXP_LIKE
The system SHALL implement `REGEXP_LIKE(string, pattern) → BOOLEAN`, true when the pattern matches anywhere in the string. A constant pattern SHALL use Arrow's vectorised `regexp_is_match_scalar` kernel.

#### Scenario: Unanchored match
- **WHEN** `REGEXP_LIKE(s, '\d+')` is evaluated where `s` is `['abc123', 'xyz', NULL]`
- **THEN** it returns `[true, false, NULL]`

### Requirement: REGEXP_EXTRACT
The system SHALL implement `REGEXP_EXTRACT(string, pattern [, group]) → VARCHAR` returning the first match (or the given capturing group). It SHALL return NULL when there is no match and SHALL error when `group` exceeds the pattern's group count.

#### Scenario: Capturing group
- **WHEN** `REGEXP_EXTRACT('1a 2b 14m', '(\d+)([a-z]+)', 2)` is evaluated
- **THEN** it returns `'a'`

#### Scenario: No match
- **WHEN** `REGEXP_EXTRACT('none', '\d+')` is evaluated
- **THEN** it returns NULL

### Requirement: REGEXP_REPLACE
The system SHALL implement `REGEXP_REPLACE(string, pattern [, replacement]) → VARCHAR` replacing every match (default replacement: empty string). Java/Trino replacement syntax (`$1`, `${name}`, `\$`) SHALL be supported.

#### Scenario: Group reference in replacement
- **WHEN** `REGEXP_REPLACE('1a 2b 14m', '(\d+)([ab]) ', '3c$2 ')` is evaluated
- **THEN** it returns `'3ca 3cb 14m'`

### Requirement: REGEXP_COUNT
The system SHALL implement `REGEXP_COUNT(string, pattern) → BIGINT` returning the number of non-overlapping matches.

#### Scenario: Count matches
- **WHEN** `REGEXP_COUNT('1a 2b 14m', '\d+')` is evaluated
- **THEN** it returns `3`
