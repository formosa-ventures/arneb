//! ScalarFunction trait and FunctionRegistry.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arrow::array::ArrayRef;

/// A scalar function that operates on Arrow arrays.
pub trait ScalarFunction: Send + Sync + Debug {
    /// The function name (uppercase canonical form).
    fn name(&self) -> &str;

    /// Infer the return type given argument types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError>;

    /// Evaluate the function on columnar arguments, producing a columnar result.
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError>;

    /// Evaluate the function for a batch of `num_rows` rows.
    ///
    /// This is the entry point the expression evaluator uses. The
    /// default forwards to [`ScalarFunction::evaluate`]; nullary
    /// functions (`now()`, `pi()`, `random()`, ...) override it because
    /// they have no argument array to take the batch length from.
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        let _ = num_rows;
        self.evaluate(args)
    }
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

    /// Register `alias` as an additional name for an already-registered
    /// function `target`. No-op when `target` is unknown.
    pub fn register_alias(&mut self, alias: &str, target: &str) {
        if let Some(f) = self.functions.get(&target.to_lowercase()).cloned() {
            self.functions.insert(alias.to_lowercase(), f);
        }
    }

    /// All registered names (lowercase, including aliases), sorted.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.functions.keys().cloned().collect();
        names.sort();
        names
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

    // Conditional functions (GREATEST / LEAST). IF is desugared to CASE
    // in the parser and TRY is handled by the expression evaluator.
    for f in super::conditional::all_conditional_functions() {
        reg.register(f);
    }

    // Regular-expression functions
    for f in super::regexp::all_regexp_functions() {
        reg.register(f);
    }

    // Trino-compatible alternate spellings.
    for (alias, target) in [
        ("CEILING", "CEIL"),
        ("POW", "POWER"),
        ("RAND", "RANDOM"),
        ("DAY_OF_MONTH", "DAY"),
        ("DOW", "DAY_OF_WEEK"),
        ("DOY", "DAY_OF_YEAR"),
        ("WEEK_OF_YEAR", "WEEK"),
        ("YOW", "YEAR_OF_WEEK"),
        ("CURRENT_TIMESTAMP", "NOW"),
        ("LOCALTIMESTAMP", "NOW"),
    ] {
        reg.register_alias(alias, target);
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

    /// Every function and alias added by trino-functions-batch1.
    const BATCH1: &[&str] = &[
        // conditional
        "greatest",
        "least",
        // string
        "split_part",
        "starts_with",
        "strpos",
        "reverse",
        "lpad",
        "rpad",
        "chr",
        "codepoint",
        "concat_ws",
        "levenshtein_distance",
        "hamming_distance",
        "translate",
        // regexp
        "regexp_like",
        "regexp_extract",
        "regexp_replace",
        "regexp_count",
        // math
        "sqrt",
        "cbrt",
        "exp",
        "ln",
        "log2",
        "log10",
        "log",
        "sign",
        "truncate",
        "pi",
        "e",
        "random",
        "rand",
        "degrees",
        "radians",
        "sin",
        "cos",
        "tan",
        "asin",
        "acos",
        "atan",
        "atan2",
        "sinh",
        "cosh",
        "tanh",
        "is_nan",
        "is_finite",
        "is_infinite",
        "nan",
        "infinity",
        "ceiling",
        "pow",
        // date / time
        "year",
        "quarter",
        "month",
        "week",
        "week_of_year",
        "day",
        "day_of_month",
        "day_of_week",
        "dow",
        "day_of_year",
        "doy",
        "year_of_week",
        "yow",
        "hour",
        "minute",
        "second",
        "millisecond",
        "date_add",
        "date_diff",
        "last_day_of_month",
        "now",
        "current_timestamp",
        "localtimestamp",
        "from_unixtime",
        "to_unixtime",
        "date",
        "date_format",
        "date_parse",
        "format_datetime",
    ];

    #[test]
    fn registry_has_trino_batch1_functions_and_aliases() {
        let reg = default_registry();
        for name in BATCH1 {
            assert!(reg.get(name).is_some(), "missing function: {name}");
        }
        assert_eq!(reg.get("ceiling").unwrap().name(), "CEIL");
        assert_eq!(reg.get("dow").unwrap().name(), "DAY_OF_WEEK");
        assert_eq!(reg.get("current_timestamp").unwrap().name(), "NOW");
    }

    /// The planner's `function_return_type` table and each function's
    /// `ScalarFunction::return_type` must agree: `ProjectionExec` casts
    /// every evaluated column to the planned type, so a mismatch would
    /// silently convert results.
    #[test]
    fn planner_and_registry_return_types_agree() {
        use arneb_common::types::{ColumnInfo, TimeUnit};
        use arneb_planner::analyzer::plan_expr_type;
        use arneb_planner::PlanExpr;

        let ts = DataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let s = DataType::Utf8;
        let i = DataType::Int64;
        let i32t = DataType::Int32;
        let f = DataType::Float64;
        let d = DataType::Date32;
        let dec = DataType::Decimal128 {
            precision: 12,
            scale: 2,
        };
        let cases: Vec<(&str, Vec<DataType>)> = vec![
            ("greatest", vec![i32t.clone(), i.clone()]),
            ("least", vec![i.clone(), f.clone()]),
            ("greatest", vec![dec.clone(), dec.clone()]),
            ("split_part", vec![s.clone(), s.clone(), i.clone()]),
            ("starts_with", vec![s.clone(), s.clone()]),
            ("strpos", vec![s.clone(), s.clone()]),
            ("reverse", vec![s.clone()]),
            ("lpad", vec![s.clone(), i.clone(), s.clone()]),
            ("rpad", vec![s.clone(), i.clone(), s.clone()]),
            ("chr", vec![i.clone()]),
            ("codepoint", vec![s.clone()]),
            ("concat_ws", vec![s.clone(), s.clone(), s.clone()]),
            ("levenshtein_distance", vec![s.clone(), s.clone()]),
            ("hamming_distance", vec![s.clone(), s.clone()]),
            ("translate", vec![s.clone(), s.clone(), s.clone()]),
            ("regexp_like", vec![s.clone(), s.clone()]),
            ("regexp_extract", vec![s.clone(), s.clone()]),
            ("regexp_replace", vec![s.clone(), s.clone(), s.clone()]),
            ("regexp_count", vec![s.clone(), s.clone()]),
            ("sqrt", vec![i.clone()]),
            ("ln", vec![dec.clone()]),
            ("log", vec![f.clone(), f.clone()]),
            ("atan2", vec![f.clone(), f.clone()]),
            ("pi", vec![]),
            ("e", vec![]),
            ("nan", vec![]),
            ("infinity", vec![]),
            ("is_nan", vec![f.clone()]),
            ("sign", vec![i32t.clone()]),
            ("sign", vec![dec.clone()]),
            ("truncate", vec![f.clone()]),
            ("truncate", vec![i.clone(), i.clone()]),
            ("random", vec![]),
            ("rand", vec![i.clone()]),
            ("year", vec![d.clone()]),
            ("day_of_week", vec![ts.clone()]),
            ("dow", vec![d.clone()]),
            ("hour", vec![ts.clone()]),
            ("millisecond", vec![ts.clone()]),
            ("date_trunc", vec![s.clone(), d.clone()]),
            ("date_trunc", vec![s.clone(), ts.clone()]),
            ("date_add", vec![s.clone(), i.clone(), d.clone()]),
            ("date_add", vec![s.clone(), i.clone(), ts.clone()]),
            ("date_diff", vec![s.clone(), d.clone(), d.clone()]),
            ("last_day_of_month", vec![d.clone()]),
            ("now", vec![]),
            ("current_timestamp", vec![]),
            ("from_unixtime", vec![f.clone()]),
            ("to_unixtime", vec![ts.clone()]),
            ("date", vec![s.clone()]),
            ("date_format", vec![ts.clone(), s.clone()]),
            ("date_parse", vec![s.clone(), s.clone()]),
            ("format_datetime", vec![ts.clone(), s.clone()]),
            ("extract", vec![s.clone(), ts.clone()]),
            ("current_date", vec![]),
        ];
        let reg = default_registry();
        for (name, arg_types) in cases {
            let schema: Vec<ColumnInfo> = arg_types
                .iter()
                .enumerate()
                .map(|(idx, t)| ColumnInfo {
                    name: format!("c{idx}"),
                    data_type: t.clone(),
                    nullable: true,
                })
                .collect();
            let expr = PlanExpr::Function {
                name: name.to_string(),
                args: (0..arg_types.len())
                    .map(|idx| PlanExpr::Column {
                        index: idx,
                        name: format!("c{idx}"),
                        span: None,
                    })
                    .collect(),
                distinct: false,
                span: None,
            };
            let planned = plan_expr_type(&expr, &schema)
                .unwrap_or_else(|| panic!("planner has no return type for {name}"));
            let executed = reg.get(name).unwrap().return_type(&arg_types).unwrap();
            assert_eq!(planned, executed, "{name}({arg_types:?})");
        }
    }
}
