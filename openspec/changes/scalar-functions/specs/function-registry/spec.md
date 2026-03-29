## ADDED Requirements

### Requirement: ScalarFunction trait
The system SHALL define a `ScalarFunction` trait with three methods: `fn name(&self) -> &str` returning the function name, `fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>` returning the output data type given input types, and `fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef>` performing vectorized evaluation on Arrow arrays. The trait SHALL require `Send + Sync` bounds.

#### Scenario: Function returns its name
- **WHEN** `name()` is called on an `UpperFunction` instance
- **THEN** it returns `"upper"`

#### Scenario: Function infers return type
- **WHEN** `return_type(&[DataType::Utf8])` is called on an `UpperFunction` instance
- **THEN** it returns `Ok(DataType::Utf8)`

#### Scenario: Function rejects invalid argument types
- **WHEN** `return_type(&[DataType::Int32])` is called on an `UpperFunction` instance
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))` indicating a type mismatch

### Requirement: FunctionRegistry
The system SHALL implement a `FunctionRegistry` struct wrapping a `HashMap<String, Arc<dyn ScalarFunction>>`. Function names SHALL be stored in lowercase for case-insensitive lookup. The registry SHALL provide `register(func: Arc<dyn ScalarFunction>)` and `get(name: &str) -> Option<Arc<dyn ScalarFunction>>` methods.

#### Scenario: Registering and looking up a function
- **WHEN** a function named "upper" is registered and `get("UPPER")` is called
- **THEN** it returns `Some(Arc<dyn ScalarFunction>)` pointing to the registered function

#### Scenario: Looking up an unregistered function
- **WHEN** `get("nonexistent")` is called on a registry
- **THEN** it returns `None`

#### Scenario: Case-insensitive lookup
- **WHEN** a function is registered as "substring" and looked up as "SUBSTRING", "Substring", or "substring"
- **THEN** all three lookups return the same function

### Requirement: Default registry
The system SHALL provide a `default_registry()` function that returns a `FunctionRegistry` pre-populated with all built-in scalar functions: UPPER, LOWER, SUBSTRING, TRIM, LTRIM, RTRIM, CONCAT, LENGTH, REPLACE, POSITION, ABS, ROUND, CEIL, FLOOR, MOD, POWER, EXTRACT, CURRENT_DATE, DATE_TRUNC.

#### Scenario: Default registry contains all built-in functions
- **WHEN** `default_registry()` is called
- **THEN** `get("upper")`, `get("abs")`, `get("extract")`, and all other built-in names return `Some(...)`

### Requirement: Registry wired into ExecutionContext
The system SHALL add an `Arc<FunctionRegistry>` field to `ExecutionContext`. The registry SHALL be accessible during expression evaluation so that `PlanExpr::Function` nodes can be resolved to their implementations.

#### Scenario: ExecutionContext provides function registry
- **WHEN** an `ExecutionContext` is created with a default registry
- **THEN** the expression evaluator can look up and invoke any registered scalar function
