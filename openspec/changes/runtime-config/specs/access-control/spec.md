## ADDED Requirements

### Requirement: Access rules change at runtime
Under `[auth] type = "password"` the rules file SHALL only seed the access rules into the state store, and the stored rules SHALL be in force. A manager SHALL read them with `GET /api/v2/access-rules` (`{rules, from_config}`), replace them with `PUT` (the whole rules JSON, validated like the file at startup; a parse error `invalid_argument` with the parser's message) and remove them with `DELETE` (which allows everything). A change SHALL be stored before the response and SHALL apply to every statement planned after it, on pgwire, Trino, the agent API and MCP, including connections already open; a statement already running SHALL keep the rules it was checked with. The audit entry SHALL name the rules by SHA-256 only.

#### Scenario: Deny on an open connection
- **WHEN** bob has run a query on catalog `pg2` over an open pgwire connection, and a manager puts rules whose `catalogs` section gives bob `none` on `pg2`
- **THEN** bob's next statement on `pg2` fails with SQLSTATE `42501`, his `/api/v2` statement ends `failed` with `forbidden`, and `/api/v2/catalogs` no longer lists `pg2` to him, without a restart

#### Scenario: Invalid rules
- **WHEN** a manager puts rules with a `tables` rule that has a `filter` field
- **THEN** the call is `invalid_argument` naming `filter` and the rules are unchanged

### Requirement: Lockout guard
A rules change, rules clear or re-seed after which the caller would no longer hold manage SHALL be refused with `409 conflict`, hint `keep a system_information write rule for yourself, or ask a user in [access_control] admin_users`, also as a dry run. A user in `[access_control] admin_users` SHALL always keep manage.

#### Scenario: Removing your own grant
- **WHEN** ops holds manage only through a `system_information` rule and puts rules without it
- **THEN** the call is `conflict` and the rules are unchanged

#### Scenario: Listed admin
- **WHEN** root is in `admin_users` and puts rules without a `system_information` section
- **THEN** the change applies and root keeps manage
