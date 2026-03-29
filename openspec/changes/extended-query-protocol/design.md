## Context

The PostgreSQL wire protocol has two query modes: Simple Query and Extended Query. trino-alt currently implements only Simple Query via `SimpleQueryHandler`. The Extended Query protocol (`PlaceholderExtendedQueryHandler` — a pgwire no-op) is what most clients use by default. The pgwire 0.25 crate provides the `ExtendedQueryHandler` trait with default implementations for Parse, Bind, Describe, Execute, Sync, and Close — we need to implement three methods: `do_query`, `do_describe_statement`, and `do_describe_portal`, plus a `QueryParser`.

## Goals / Non-Goals

**Goals:**

- Implement Extended Query protocol so DBeaver, JDBC, psycopg2, node-postgres, pgx, and all standard PostgreSQL clients work out of the box
- Support parameter placeholders (`$1`, `$2`) via simple text substitution
- Support Describe (return column metadata and parameter types)
- Reuse the existing parse → plan → optimize → execute pipeline

**Non-Goals:**

- Binary format result encoding (text format only for now)
- Server-side prepared statement caching across connections
- Cursor support (`max_rows` chunking — execute returns all rows)
- Transaction support (BEGIN/COMMIT/ROLLBACK)

## Decisions

### D1: Statement type = SQL string

**Choice**: Use `String` as the `Statement` type for `ExtendedQueryHandler`. The QueryParser simply stores the SQL text. Parameter binding happens at execution time by replacing `$N` placeholders with text values.

**Rationale**: trino-alt's parser (`sqlparser-rs`) doesn't natively support `$N` placeholders. Replacing them with literal values before parsing is the simplest approach. pgwire's `Portal` already provides parameter values from the Bind message.

**Alternative**: Parse SQL into AST with placeholder nodes, bind at the plan level. Rejected — much more complex, not needed for correctness.

### D2: Reuse existing query execution path

**Choice**: `do_query` takes the bound SQL string (with parameters substituted) and runs the same `parse → plan → optimize → execute → encode` pipeline as `SimpleQueryHandler`.

**Rationale**: Avoids code duplication. The only difference between Simple and Extended query paths is how the SQL arrives (direct text vs. parse-then-bind). The execution pipeline is identical.

### D3: Describe returns schema from dry-run planning

**Choice**: `do_describe_statement` parses and plans the SQL (without executing) to return column metadata. `do_describe_portal` does the same but with parameters substituted.

**Rationale**: The planner's `LogicalPlan::schema()` already returns `Vec<ColumnInfo>` which maps directly to `FieldInfo` for the RowDescription message. No new infrastructure needed.

### D4: Use pgwire's default on_parse/on_bind/on_close

**Choice**: Don't override `on_parse`, `on_bind`, `on_describe`, or `on_close`. Use pgwire's default implementations which handle statement/portal storage automatically.

**Rationale**: pgwire's defaults handle the state management (storing statements in `PortalStore`, creating portals from bind messages). We only need to implement the three "do_" methods that contain the actual logic.

## Risks / Trade-offs

**[Text parameter substitution]** → Parameters are converted to text and spliced into SQL. This prevents truly parameterized queries (no plan reuse, no type safety). **Mitigation**: Acceptable for Phase 2. The primary goal is client compatibility, not prepared statement performance. Future work can add proper AST-level parameter binding.

**[No binary format]** → Some clients request binary result encoding. **Mitigation**: pgwire handles format negotiation. If a client requests binary and we only provide text, pgwire should handle the mismatch gracefully. May need to verify this.

**[Describe accuracy]** → Describe runs the planner to get schema, but some queries (e.g., with unresolvable parameters) may fail at describe time. **Mitigation**: Return an error for describe failures. Clients typically handle this gracefully.
