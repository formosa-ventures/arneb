## ADDED Requirements

### Requirement: Access control settings
The `[access_control]` section SHALL accept `rules_file`, the path of the
access rules file, overridden by `ARNEB_ACCESS_CONTROL_RULES_FILE`
(env > file). A file that cannot be read or parsed SHALL stop startup with
an error naming the path.

#### Scenario: Env override
- **WHEN** the config sets `rules_file = "/a.json"` and `ARNEB_ACCESS_CONTROL_RULES_FILE=/b.json`
- **THEN** the server loads `/b.json`
