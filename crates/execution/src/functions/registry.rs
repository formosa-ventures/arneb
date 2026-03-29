//! ScalarFunction trait and FunctionRegistry.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use trino_common::error::ExecutionError;
use trino_common::types::DataType;

/// A scalar function that operates on Arrow arrays.
pub trait ScalarFunction: Send + Sync + Debug {
    /// The function name (uppercase canonical form).
    fn name(&self) -> &str;

    /// Infer the return type given argument types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError>;

    /// Evaluate the function on columnar arguments, producing a columnar result.
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError>;
}

/// Registry of scalar functions, keyed by lowercase name for case-insensitive lookup.
#[derive(Debug)]
pub struct FunctionRegistry {
    functions: HashMap<String, Arc<dyn ScalarFunction>>,
}

impl FunctionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a scalar function. Overwrites any existing function with the same name.
    pub fn register(&mut self, func: Arc<dyn ScalarFunction>) {
        self.functions.insert(func.name().to_lowercase(), func);
    }

    /// Look up a function by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&Arc<dyn ScalarFunction>> {
        self.functions.get(&name.to_lowercase())
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a default registry pre-populated with all built-in scalar functions.
pub fn default_registry() -> FunctionRegistry {
    let mut reg = FunctionRegistry::new();

    // String functions
    for f in super::string::all_string_functions() {
        reg.register(f);
    }

    // Math functions
    for f in super::math::all_math_functions() {
        reg.register(f);
    }

    // Date functions
    for f in super::date::all_date_functions() {
        reg.register(f);
    }

    reg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_case_insensitive_lookup() {
        let reg = default_registry();
        assert!(reg.get("UPPER").is_some());
        assert!(reg.get("upper").is_some());
        assert!(reg.get("Upper").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn registry_has_all_builtin_functions() {
        let reg = default_registry();
        for name in [
            "upper",
            "lower",
            "substring",
            "trim",
            "ltrim",
            "rtrim",
            "concat",
            "length",
            "replace",
            "position",
            "abs",
            "round",
            "ceil",
            "floor",
            "mod",
            "power",
        ] {
            assert!(reg.get(name).is_some(), "missing function: {name}");
        }
    }
}
