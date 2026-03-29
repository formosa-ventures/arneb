## 1. DDLProvider Trait

- [x] 1.1 Create `DDLProvider` trait in connectors crate with methods: `create_table`, `drop_table`, `insert_into`, `delete_from`, `create_table_as_select` — all returning `Result<>`
- [x] 1.2 Add optional `fn ddl_provider(&self) -> Option<Arc<dyn DDLProvider>>` to `ConnectorFactory` trait with default `None` implementation

## 2. AST — DDL Statements

- [x] 2.1 Add `Statement::CreateTable { name, columns, if_not_exists }` to AST and map from sqlparser
- [x] 2.2 Add `Statement::DropTable { name, if_exists }` to AST and map from sqlparser
- [x] 2.3 Add `Statement::CreateTableAsSelect { name, query }` to AST and map from sqlparser

## 3. AST — DML Statements

- [x] 3.1 Add `Statement::InsertInto { table, columns, source }` where source is `Values(Vec<Vec<Expr>>)` or `Query(Box<Query>)`, map from sqlparser
- [x] 3.2 Add `Statement::DeleteFrom { table, predicate }` to AST and map from sqlparser

## 4. AST — View Statements

- [x] 4.1 Add `Statement::CreateView { name, query, or_replace }` to AST and map from sqlparser
- [x] 4.2 Add `Statement::DropView { name, if_exists }` to AST and map from sqlparser

## 5. Parsing Unit Tests

- [x] 5.1 Write tests for parsing CREATE TABLE, DROP TABLE (with IF EXISTS), CTAS
- [x] 5.2 Write tests for parsing INSERT INTO VALUES, INSERT INTO SELECT, DELETE FROM WHERE
- [x] 5.3 Write tests for parsing CREATE VIEW, CREATE OR REPLACE VIEW, DROP VIEW

## 6. Logical Plan Nodes

- [x] 6.1 Add `LogicalPlan::CreateTable { name, schema }`, `DropTable { name, if_exists }`, `CreateTableAsSelect { name, source }` variants
- [x] 6.2 Add `LogicalPlan::InsertInto { table, source }`, `DeleteFrom { table, predicate }` variants
- [x] 6.3 Add `LogicalPlan::CreateView { name, sql, plan }`, `DropView { name, if_exists }` variants

## 7. Query Planner — DDL/DML

- [x] 7.1 Plan CREATE TABLE, DROP TABLE, CTAS AST nodes to corresponding LogicalPlan nodes, resolving table references to target connector
- [x] 7.2 Plan INSERT INTO (VALUES and SELECT subquery sources) and DELETE FROM to LogicalPlan nodes

## 8. Query Planner — Views

- [x] 8.1 Plan CREATE VIEW and DROP VIEW to corresponding LogicalPlan nodes
- [x] 8.2 Extend table reference resolution to check view registry before catalog, substituting view's logical plan when found

## 9. View Registry

- [x] 9.1 Implement `ViewRegistry` in catalog: stores view definitions (name, SQL, LogicalPlan) with add, remove, lookup, list operations scoped to catalog/schema namespace

## 10. Memory Connector DDL Implementation

- [x] 10.1 Implement `create_table` for memory connector: create empty table entry, validate no duplicate, store schema
- [x] 10.2 Implement `insert_into` for memory connector: validate schema match, append batches with RwLock, return row count
- [x] 10.3 Implement `delete_from` for memory connector: truncate if no predicate, filter matching rows if predicate provided, return deleted count
- [x] 10.4 Implement `drop_table` for memory connector: remove table entry and data, error if not found

## 11. File Connector CTAS

- [x] 11.1 Implement `create_table_as_select` for file connector: write RecordBatches as Parquet with snappy compression, derive path from table name, register in file connector

## 12. Execution Wiring

- [x] 12.1 Wire DDL execution in ExecutionContext: CreateTable/DropTable/CTAS → lookup connector → call DDLProvider methods, return command-complete
- [x] 12.2 Wire DML execution in ExecutionContext: InsertInto (execute source, call insert_into), DeleteFrom (call delete_from), return row counts
- [x] 12.3 Wire view execution in ExecutionContext: CreateView → register in ViewRegistry, DropView → remove from registry

## 13. Protocol Layer

- [x] 13.1 Format DDL/DML responses as PostgreSQL CommandComplete: "CREATE TABLE", "DROP TABLE", "INSERT 0 N", "DELETE N", "CREATE VIEW", "DROP VIEW"

## 14. Integration Tests

- [x] 14.1 Test DDL: CREATE TABLE + INSERT + SELECT, DROP TABLE + SELECT error, DROP IF EXISTS, CREATE on existing error, CTAS
- [x] 14.2 Test DML: INSERT VALUES (single/multi row), INSERT SELECT, DELETE with/without WHERE, type mismatch error, column count mismatch, DML on read-only connector error
- [x] 14.3 Test Views: CREATE VIEW + SELECT, view with WHERE, view with JOIN, DROP VIEW + SELECT error, CREATE OR REPLACE, nested views, view reflects updated data
