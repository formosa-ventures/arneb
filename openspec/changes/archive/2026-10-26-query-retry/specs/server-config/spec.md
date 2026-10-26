## ADDED Requirements

### Requirement: Query retry settings
`[cluster] retry_policy` SHALL accept `none` (default) or `query`, case
insensitive, overridden by `ARNEB_RETRY_POLICY`. `[cluster]
query_retry_attempts` SHALL default to 4 and SHALL be at least 1, overridden
by `ARNEB_QUERY_RETRY_ATTEMPTS`. Any other policy, including `task`, or an
invalid count SHALL stop startup with an error naming the setting. The
coordinator SHALL log the effective policy and attempt count on
`arneb::config`.

#### Scenario: Task policy
- **WHEN** `retry_policy = "task"`
- **THEN** the coordinator refuses to start, saying `task` is not supported

#### Scenario: Env override
- **WHEN** the file sets `retry_policy = "query"` and `ARNEB_RETRY_POLICY=none` is set
- **THEN** retry is off
