//! PostgreSQL system catalog functions that pgwire clients call while
//! reading `pg_catalog` (pg-catalog-tables): `format_type`,
//! `pg_get_userbyid`, `pg_table_is_visible`, `pg_is_other_temp_schema`,
//! `obj_description`, `col_description` and `to_regtype`.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::pg_oid;
use arneb_common::types::DataType;
use arrow::array::{new_null_array, Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::DataType as ArrowDataType;

use super::args::{check_arity, int64, utf8};
use super::registry::ScalarFunction;

pub(crate) fn all_pg_catalog_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(FormatTypeFunction),
        Arc::new(ToRegtypeFunction),
        Arc::new(GetUserByIdFunction),
        Arc::new(ConstantBoolFunction {
            name: "PG_TABLE_IS_VISIBLE",
            value: true,
        }),
        Arc::new(ConstantBoolFunction {
            name: "PG_IS_OTHER_TEMP_SCHEMA",
            value: false,
        }),
        Arc::new(NoDescriptionFunction {
            name: "OBJ_DESCRIPTION",
            min_args: 1,
        }),
        Arc::new(NoDescriptionFunction {
            name: "COL_DESCRIPTION",
            min_args: 2,
        }),
    ]
}

/// `format_type(type_oid, typmod)`: the SQL name of a type
/// ([`pg_oid::format_type`]); NULL for a NULL oid.
#[derive(Debug)]
struct FormatTypeFunction;

impl ScalarFunction for FormatTypeFunction {
    fn name(&self) -> &str {
        "FORMAT_TYPE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("FORMAT_TYPE", args, 2, 2)?;
        let oids = int64(&args[0], "FORMAT_TYPE")?;
        let typmods = int64(&args[1], "FORMAT_TYPE")?;
        let result: StringArray = (0..oids.len())
            .map(|i| {
                (!oids.is_null(i)).then(|| {
                    let typmod = (!typmods.is_null(i)).then(|| typmods.value(i));
                    pg_oid::format_type(oids.value(i), typmod)
                })
            })
            .collect();
        Ok(Arc::new(result))
    }
}

/// `to_regtype(name)`: the oid of a built-in type, or NULL when there is
/// none of that name ([`pg_oid::type_oid`]).
#[derive(Debug)]
struct ToRegtypeFunction;

impl ScalarFunction for ToRegtypeFunction {
    fn name(&self) -> &str {
        "TO_REGTYPE"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("TO_REGTYPE", args, 1, 1)?;
        let names = utf8(&args[0], "TO_REGTYPE")?;
        let result: Int64Array = names.iter().map(|n| n.and_then(pg_oid::type_oid)).collect();
        Ok(Arc::new(result))
    }
}

/// `pg_get_userbyid(role_oid)`: the owner role's name, or PostgreSQL's
/// `unknown (OID=n)` for any other oid.
#[derive(Debug)]
struct GetUserByIdFunction;

impl ScalarFunction for GetUserByIdFunction {
    fn name(&self) -> &str {
        "PG_GET_USERBYID"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("PG_GET_USERBYID", args, 1, 1)?;
        let oids = int64(&args[0], "PG_GET_USERBYID")?;
        let result: StringArray = oids
            .iter()
            .map(|oid| {
                oid.map(|oid| {
                    if oid == pg_oid::OWNER_ROLE_OID {
                        pg_oid::OWNER_ROLE_NAME.to_string()
                    } else {
                        format!("unknown (OID={oid})")
                    }
                })
            })
            .collect();
        Ok(Arc::new(result))
    }
}

/// A one-argument predicate with the same answer for every non-NULL oid:
/// every relation is visible, and no schema is another session's
/// temporary schema.
#[derive(Debug)]
struct ConstantBoolFunction {
    name: &'static str,
    value: bool,
}

impl ScalarFunction for ConstantBoolFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Boolean)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, 1, 1)?;
        let arg = &args[0];
        let result: BooleanArray = (0..arg.len())
            .map(|i| (!arg.is_null(i)).then_some(self.value))
            .collect();
        Ok(Arc::new(result))
    }
}

/// `obj_description` / `col_description`: Arneb stores no comments, so
/// always NULL.
#[derive(Debug)]
struct NoDescriptionFunction {
    name: &'static str,
    min_args: usize,
}

impl ScalarFunction for NoDescriptionFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, self.min_args, 2)?;
        Ok(new_null_array(&ArrowDataType::Utf8, args[0].len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn format_type_names_types() {
        let out = FormatTypeFunction
            .evaluate(&[
                ints(&[Some(23), Some(1700), None]),
                ints(&[Some(-1), Some(pg_oid::numeric_typmod(12, 2)), Some(-1)]),
            ])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "integer");
        assert_eq!(out.value(1), "numeric(12,2)");
        assert!(out.is_null(2));
    }

    #[test]
    fn to_regtype_finds_builtin_types() {
        let out = ToRegtypeFunction
            .evaluate(&[Arc::new(StringArray::from(vec![
                Some("int4"),
                Some("hstore"),
                None,
            ]))])
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 23);
        assert!(out.is_null(1) && out.is_null(2));
    }

    #[test]
    fn owner_and_predicates() {
        let out = GetUserByIdFunction
            .evaluate(&[ints(&[Some(pg_oid::OWNER_ROLE_OID), Some(7)])])
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "arneb");
        assert_eq!(out.value(1), "unknown (OID=7)");

        let visible = ConstantBoolFunction {
            name: "PG_TABLE_IS_VISIBLE",
            value: true,
        }
        .evaluate(&[ints(&[Some(1), None])])
        .unwrap();
        let visible = visible.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(visible.value(0));
        assert!(visible.is_null(1));

        let none = NoDescriptionFunction {
            name: "COL_DESCRIPTION",
            min_args: 2,
        }
        .evaluate(&[ints(&[Some(1)]), ints(&[Some(1)])])
        .unwrap();
        assert_eq!(none.null_count(), 1);
    }
}
