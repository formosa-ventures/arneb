## ADDED Requirements

### Requirement: Argument normalisation for string functions
String functions SHALL accept NULL literals, `LargeUtf8` and (for integer positions) any integer type by casting arguments with Arrow's `cast` kernel, and SHALL NOT panic on such inputs.

#### Scenario: NULL literal
- **WHEN** `UPPER(NULL)` is evaluated
- **THEN** it returns NULL

### Requirement: SPLIT_PART
The system SHALL implement `SPLIT_PART(string, delimiter, index) → VARCHAR` returning the 1-based `index`-th field. It SHALL return NULL when `index` exceeds the field count and SHALL error when `index <= 0`. An empty delimiter SHALL split into single characters.

#### Scenario: Field past the end
- **WHEN** `SPLIT_PART('a,b,c', ',', 4)` is evaluated
- **THEN** it returns NULL

### Requirement: STARTS_WITH, STRPOS, REVERSE
The system SHALL implement `STARTS_WITH(string, prefix) → BOOLEAN` (Arrow `starts_with` kernel), `STRPOS(string, substring [, instance]) → BIGINT` (1-based character position of the n-th occurrence, 0 when absent, error when `instance <= 0`), and `REVERSE(string) → VARCHAR` (code-point reversal).

#### Scenario: STRPOS instance
- **WHEN** `STRPOS('abcabc', 'bc', 2)` is evaluated
- **THEN** it returns `5`

### Requirement: LPAD and RPAD
The system SHALL implement `LPAD`/`RPAD(string, size, padstring) → VARCHAR`, padding to `size` characters with repeated `padstring` and truncating to `size` when the string is longer. Negative `size` or empty `padstring` SHALL error.

#### Scenario: Pad and truncate
- **WHEN** `LPAD('42', 5, '0')` and `LPAD('hello', 3, '*')` are evaluated
- **THEN** they return `'00042'` and `'hel'`

### Requirement: CHR, CODEPOINT, CONCAT_WS, TRANSLATE
The system SHALL implement `CHR(n) → VARCHAR` and `CODEPOINT(c) → INTEGER` (errors on invalid code points / non-single-character input), `CONCAT_WS(sep, ...) → VARCHAR` (skips NULL arguments, NULL only when `sep` is NULL), and `TRANSLATE(source, from, to) → VARCHAR` (characters of `from` without a counterpart are deleted; first mapping wins).

#### Scenario: CONCAT_WS skips NULLs
- **WHEN** `CONCAT_WS(',', 'a', NULL, 'b')` is evaluated
- **THEN** it returns `'a,b'`

### Requirement: Edit distances
The system SHALL implement `LEVENSHTEIN_DISTANCE(a, b) → BIGINT` and `HAMMING_DISTANCE(a, b) → BIGINT`; the latter SHALL error when the strings differ in length.

#### Scenario: Levenshtein
- **WHEN** `LEVENSHTEIN_DISTANCE('kitten', 'sitting')` is evaluated
- **THEN** it returns `3`

## MODIFIED Requirements

### Requirement: TRIM, LTRIM, RTRIM
The system SHALL implement `TrimFunction`, `LtrimFunction`, and `RtrimFunction` that remove whitespace — or, when a second argument is given, any of the characters in that argument — from both ends, the left end, or the right end of strings respectively. Null values SHALL be propagated.

#### Scenario: TRIM removes leading and trailing whitespace
- **WHEN** `TRIM` is evaluated with input `["  hello  ", " world ", NULL]`
- **THEN** it returns `["hello", "world", NULL]`

#### Scenario: TRIM with a character set
- **WHEN** `TRIM('xxhixyx', 'xy')` is evaluated
- **THEN** it returns `'hi'`

### Requirement: REPLACE
The system SHALL implement `REPLACE(string, search [, replacement])`. The 2-argument form SHALL remove every occurrence of `search`.

#### Scenario: Two-argument REPLACE
- **WHEN** `REPLACE('a-b-c', '-')` is evaluated
- **THEN** it returns `'abc'`
