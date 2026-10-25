## 1. Rules (crates/catalog)

- [x] 1.1 `access` module: rule types (serde, `deny_unknown_fields`, legacy `allow: bool`), anchored regex compile, `AccessRules::from_json` with errors naming the offending section/field
- [x] 1.2 Checks: `catalog_access`, `can_select`, `catalog_visible`, `schema_visible`, `table_visible`, `can_view_query`; absent section = allow
- [x] 1.3 `CatalogManager::with_access`, access-checked `resolve_table`, `visible_*_names`; `CatalogError::AccessDenied`
- [x] 1.4 Unit tests: first match, no match denies, absent sections, unsupported fields, legacy allow, unqualified names, denied-before-lookup

## 2. Protocols (crates/protocol)

- [x] 2.1 pgwire: wrap `session_catalog` with the connection user; metadata handlers use `visible_*`
- [x] 2.2 Trino: wrap the per-query view with `ClientSession.user`; `SHOW`, virtual `information_schema` / `system.jdbc` use `visible_*`
- [x] 2.3 `system.runtime.queries` filtered by `can_view_query`; statements refused without `execute`
- [x] 2.4 Error mapping: `42501` / `PERMISSION_DENIED`
- [x] 2.5 Tests on both protocols: denied read, filtered listings, query visibility

## 3. Server

- [x] 3.1 `[access_control] rules_file` + `ARNEB_ACCESS_CONTROL_RULES_FILE`; load at startup, fail on error
- [x] 3.2 Startup log of the policy; warning with `[auth] type = "none"`

## 4. Validation and docs

- [x] 4.1 Tier 1: workspace tests (1667 passed, 0 failed), clippy `-D warnings`, fmt
- [x] 4.2 Tier 2 (2026-10-25, native coordinator + 2 workers on TPC-H SF1 Parquet, password auth, TLS on pgwire and Trino, a 7-rule file): alice reads `nation`/`region` (a distributed join works) and is refused `lineitem` on pgwire (`42501`) and `orders` on Trino (`PERMISSION_DENIED`); `information_schema.tables` and Trino `SHOW TABLES` list only `nation`, `region` for her and all 8 tables for admin; `system.runtime.queries` shows alice only her own queries and admin both users'; startup logs `access control rules loaded` with the section sizes.
- [x] 4.3 `docs/guide/access-control.md`, `configuration.md`, sidebar, roadmap status
