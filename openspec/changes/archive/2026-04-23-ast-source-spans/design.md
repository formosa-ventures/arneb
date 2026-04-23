# Design: ast-source-spans

## Goals

- Every diagnostic Arneb emits for user-submitted SQL can point at the exact source location of the offending construct.
- Output quality matches modern compiler UX (rustc/clang) — line:col prefix AND a source snippet with underline carets.
- Span propagation is uniform: once established in AST, it flows unchanged through AST conversion, logical planning, analyzer passes, and optimizer passes.
- Zero friction for consumers who don't care about positions — `location()` is optional; absent values render as position-free errors.

## Non-goals

- Not adding span-aware formatting to `tracing` logs (internal debug output keeps its own conventions).
- Not tracking span information on physical plan nodes (`ExecutionPlan`) — once planning completes, spans are consumed by the error path, not the execution path.
- Not covering spans on catalog objects (table DDL storage) — catalog metadata is stored independently of any SQL source.

## Key Decisions

### D1. Span stored as inline field on every AST variant

```rust
pub enum Expr {
    Column { col_ref: ColumnRef, span: Span },
    Literal { value: ScalarValue, span: Span },
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr>, span: Span },
    // ... every variant carries span
}
```

**Why A over B/C**: matches precedent from rustc HIR, Spark Catalyst `TreeNode.origin`, and Trino `Node.location`. Pattern matches get a mechanical `span: _` line but everything else stays local. Wrapper (`Spanned<Expr>`) forces all recursive traversals to handle the wrapper explicitly. Side-table (`HashMap<ExprId, Span>`) requires introducing stable IDs, which sqlparser-rs doesn't provide and Arneb currently doesn't need.

**Trade**: every `ast::Expr::X { ... }` constructor site must now pass a span. For tests and fixture builders, an `Expr::X_at(..., Span::empty())` helper keeps boilerplate down.

### D2. PlanExpr span is `Option<Span>`

Analyzer and optimizer passes synthesize new nodes (e.g., `Cast` inserted by type coercion, `BinaryOp::And` split during predicate pushdown) that have no user-visible source location. Rather than invent a fake span, those synthetic nodes use `None`. Error reporters fall back to the nearest enclosing `Some(_)` when a synthetic node is the root cause.

```rust
pub enum PlanExpr {
    Column { index: usize, ..., span: Option<Span> },
    Cast { expr: Box<PlanExpr>, data_type: DataType, span: Option<Span> },
    // ...
}

impl PlanExpr {
    pub fn best_span(&self) -> Option<Span> {
        self.span().or_else(|| self.children().iter().find_map(|c| c.best_span()))
    }
}
```

### D3. Span coverage

MVP covers the nodes whose errors are user-facing today or in the next two changes:

- All `ast::Expr` variants (type mismatch, unknown column, function resolution errors attach here)
- All `ast::Statement` variants (statement-level errors: unsupported statement, DDL target missing)
- `ast::ColumnRef` (column-not-found, ambiguous reference)

Nodes deferred to a follow-up change: `TableFactor`, `JoinOperator`, `SelectItem`, `OrderByExpr`. These get `Span::empty()` placeholders in convert.rs now and can be plumbed properly when their errors need location context.

### D4. Rustc-style error rendering via `codespan-reporting`

Crate choice: [`codespan-reporting`](https://crates.io/crates/codespan-reporting) (BSD-3, widely adopted). It provides a `Diagnostic` builder and a terminal formatter that produces output like:

```
error: cannot apply operator '<=' to Utf8 and Date32
  ┌─ query.sql:3:19
  │
3 │   WHERE l_shipdate <= DATE '1998-12-01'
  │                    ^^ here
  │
  = hint: use CAST(l_shipdate AS DATE)
```

Integration layer:

```rust
// crates/common/src/diagnostic.rs
pub struct SourceFile {
    pub name: String,       // "<query>" for ad-hoc, path for file-sourced
    pub text: String,       // the SQL string
}

pub fn render_plan_error(err: &PlanError, source: &SourceFile) -> String { ... }
```

For environments without source context (unit tests, programmatic API with lost source), the renderer falls back to `line X:Y: <message>` or just `<message>`.

### D5. Error variant structure

Each `PlanError` variant carries `location: Option<Location>` where applicable. `Location` is the re-exported `sqlparser::tokenizer::Location` (we stay compatible with the upstream type rather than re-inventing one).

```rust
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("cannot apply operator '{op}' to {left_type} and {right_type}")]
    TypeMismatch {
        op: String,
        left_type: DataType,
        right_type: DataType,
        location: Option<Location>,
    },
    #[error("column '{name}' not found")]
    ColumnNotFound {
        name: String,
        location: Option<Location>,
    },
    // ...
}

impl PlanError {
    pub fn location(&self) -> Option<Location> {
        match self {
            Self::TypeMismatch { location, .. } => *location,
            Self::ColumnNotFound { location, .. } => *location,
            _ => None,
        }
    }
}
```

The `Display` impl produced by `thiserror` keeps the current `{message}` template; the location prefix is applied by `render_plan_error`, not by `Display`. This keeps the programmatic `err.to_string()` useful for code that doesn't care about position (test assertions, log fields).

## Source propagation

```
┌────────────────┐
│ SQL string     │ ──► Parser ──► sqlparser AST (spans built-in)
└────────────────┘                       │
                                          ▼ convert.rs
┌─────────────────────────────────────────┐
│ arneb::ast::{Expr,Statement,ColumnRef}  │  ← span: Span on every variant
└─────────────────────────────────────────┘
            │
            ▼ QueryPlanner
┌─────────────────────────────────────────┐
│ PlanExpr / LogicalPlan                  │  ← span: Option<Span>
└─────────────────────────────────────────┘
            │
            ▼ Analyzer / Optimizer
     (synthetic nodes have span=None)
            │
            ▼
       PlanError { location: Option<Location> }
            │
            ▼ render_plan_error(&err, &source)
       ┌──────────────────────────────┐
       │ rustc-style diagnostic output │
       └──────────────────────────────┘
```

`SourceFile` is threaded down the protocol/server layer (`crates/protocol`) so pgwire error responses can render rich diagnostics. When source is unavailable (e.g. internal API calls that skip the string stage), `Display` falls back to the position-free message.

## Open questions

- **Multi-statement scripts**: if a client submits `SELECT ...; SELECT ...;` and the second fails, which source do we highlight? Default plan: each statement carries its own span; the outermost driver knows which statement produced the error.
- **Span arithmetic on converted expressions**: when sqlparser's `Expr::Cast { ... }` is lowered into Arneb's `Cast` + inner `Literal`, both nodes get the same outer span. Acceptable for MVP.
- **`serde` serialization of spans**: spans leak into `EXPLAIN (FORMAT JSON)` if serialized. Decision: skip span serialization via `#[serde(skip)]` — EXPLAIN output stays stable.

## Rejected alternatives

- **Wrapper `Spanned<T>`**: considered but rejected because every recursive AST traversal (visitor, convert, plan_expr) would need a `spanned.inner` deref. Inline fields keep the consumer code shape.
- **Side-table `HashMap<ExprId, Span>`**: considered for zero AST invasion; rejected because sqlparser-rs AST nodes are identityless (structural `PartialEq`) and we'd need to invent stable IDs.
- **Custom `Location` type**: considered for type ownership; rejected because sqlparser-rs's `Location` is zero-cost, `Copy`, and already tracked in every upstream token.

## Migration

- Existing tests that assert error strings: update to match new location-prefixed format where `SourceFile` is available. Where not available, assertions are unchanged.
- Callers constructing `ast::Expr::X` manually: update to pass a span. Use `Span::empty()` in synthetic callers.
- No DB/wire-protocol compatibility concerns — pgwire error responses are backward compatible (add location to the message body only).
