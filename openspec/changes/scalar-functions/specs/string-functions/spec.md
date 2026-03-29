## ADDED Requirements

### Requirement: UPPER and LOWER
The system SHALL implement `UpperFunction` and `LowerFunction` that convert Utf8 arrays to uppercase and lowercase respectively. They SHALL accept exactly one Utf8 argument and return a Utf8 array. They SHALL use Arrow compute kernels (`upper`, `lower`). Null values SHALL be propagated.

#### Scenario: UPPER converts to uppercase
- **WHEN** `UPPER` is evaluated with input `["hello", "World", NULL]`
- **THEN** it returns `["HELLO", "WORLD", NULL]`

#### Scenario: LOWER converts to lowercase
- **WHEN** `LOWER` is evaluated with input `["Hello", "WORLD", NULL]`
- **THEN** it returns `["hello", "world", NULL]`

#### Scenario: UPPER with empty string
- **WHEN** `UPPER` is evaluated with input `["", "abc"]`
- **THEN** it returns `["", "ABC"]`

### Requirement: SUBSTRING
The system SHALL implement `SubstringFunction` that extracts a substring from a Utf8 array. It SHALL accept two or three arguments: `SUBSTRING(str, start)` or `SUBSTRING(str, start, length)`. Start is 1-based per SQL convention. It SHALL use Arrow's `substring` kernel. Null values SHALL be propagated.

#### Scenario: SUBSTRING with start and length
- **WHEN** `SUBSTRING("hello world", 7, 5)` is evaluated
- **THEN** it returns `"world"`

#### Scenario: SUBSTRING with only start
- **WHEN** `SUBSTRING("hello world", 7)` is evaluated
- **THEN** it returns `"world"`

#### Scenario: SUBSTRING with null input
- **WHEN** `SUBSTRING(NULL, 1, 3)` is evaluated
- **THEN** it returns `NULL`

### Requirement: TRIM, LTRIM, RTRIM
The system SHALL implement `TrimFunction`, `LtrimFunction`, and `RtrimFunction` that remove whitespace from both ends, the left end, or the right end of strings respectively. Each SHALL accept exactly one Utf8 argument and return a Utf8 array. Null values SHALL be propagated.

#### Scenario: TRIM removes leading and trailing whitespace
- **WHEN** `TRIM` is evaluated with input `["  hello  ", " world ", NULL]`
- **THEN** it returns `["hello", "world", NULL]`

#### Scenario: LTRIM removes leading whitespace only
- **WHEN** `LTRIM` is evaluated with input `["  hello  "]`
- **THEN** it returns `["hello  "]`

#### Scenario: RTRIM removes trailing whitespace only
- **WHEN** `RTRIM` is evaluated with input `["  hello  "]`
- **THEN** it returns `["  hello"]`

### Requirement: CONCAT and LENGTH
The system SHALL implement `ConcatFunction` that concatenates two or more Utf8 arguments element-wise, returning a Utf8 array. It SHALL accept a variable number of arguments (minimum 2). Null values in any argument SHALL produce a null in the result. The system SHALL implement `LengthFunction` that returns the character length of each string as an Int64 array. It SHALL accept exactly one Utf8 argument.

#### Scenario: CONCAT two strings
- **WHEN** `CONCAT("hello", " world")` is evaluated
- **THEN** it returns `"hello world"`

#### Scenario: CONCAT with null
- **WHEN** `CONCAT("hello", NULL)` is evaluated
- **THEN** it returns `NULL`

#### Scenario: LENGTH of strings
- **WHEN** `LENGTH` is evaluated with input `["hello", "", NULL]`
- **THEN** it returns `[5, 0, NULL]`

### Requirement: REPLACE and POSITION
The system SHALL implement `ReplaceFunction` that replaces all occurrences of a substring within a string. It SHALL accept three Utf8 arguments: `REPLACE(str, from, to)`. The system SHALL implement `PositionFunction` that returns the 1-based position of the first occurrence of a substring within a string, or 0 if not found. It SHALL accept two Utf8 arguments: `POSITION(substr, str)`. Both SHALL propagate null values.

#### Scenario: REPLACE substitutes substring
- **WHEN** `REPLACE("hello world", "world", "rust")` is evaluated
- **THEN** it returns `"hello rust"`

#### Scenario: REPLACE with no match
- **WHEN** `REPLACE("hello", "xyz", "abc")` is evaluated
- **THEN** it returns `"hello"`

#### Scenario: POSITION finds substring
- **WHEN** `POSITION("world", "hello world")` is evaluated
- **THEN** it returns `7`

#### Scenario: POSITION substring not found
- **WHEN** `POSITION("xyz", "hello")` is evaluated
- **THEN** it returns `0`

#### Scenario: POSITION with null
- **WHEN** `POSITION(NULL, "hello")` is evaluated
- **THEN** it returns `NULL`
