//! PostgreSQL system catalog functions that pgwire clients call while
//! reading `pg_catalog` (pg-catalog-tables): `format_type`,
//! `pg_get_userbyid`, `pg_table_is_visible`, `pg_is_other_temp_schema`,
//! `obj_description`, `col_description` and `to_regtype`; the constraint,
//! index, default and sequence definition functions SQLAlchemy reflection
//! calls (Arneb has none of those objects, so they return NULL); and the
//! runtime `regclass` conversions the planner emits (`__regclass_name`,
//! `__regclass_oid`); and `current_setting`.

use std::collections::HashMap;
use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::pg_oid;
use arneb_common::pg_settings;
use arneb_common::types::DataType;
use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Int64Array, ListBuilder, StringArray,
    StringBuilder,
};
use arrow::datatypes::DataType as ArrowDataType;

use super::args::{check_arity, int64, invalid, utf8};
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
        Arc::new(NullTextFunction {
            name: "PG_GET_CONSTRAINTDEF",
            min_args: 1,
            max_args: 2,
        }),
        Arc::new(NullTextFunction {
            name: "PG_GET_EXPR",
            min_args: 2,
            max_args: 3,
        }),
        Arc::new(NullTextFunction {
            name: "PG_GET_INDEXDEF",
            min_args: 1,
            max_args: 3,
        }),
        Arc::new(NullTextFunction {
            name: "PG_GET_SERIAL_SEQUENCE",
            min_args: 2,
            max_args: 2,
        }),
        Arc::new(ConstantBoolFunction {
            name: "PG_COLLATION_IS_VISIBLE",
            value: true,
        }),
        Arc::new(RegclassName),
        Arc::new(RegclassOid),
        Arc::new(CurrentSettingFunction),
        Arc::new(CurrentSchemasFunction),
        Arc::new(FormatFunction),
    ]
}

/// PostgreSQL's reserved, column-name and type/function-name keywords: the
/// words `quote_ident` (and so `format('%I')`) double-quotes. Hardcoded from
/// PostgreSQL's keyword table (sqlparser's keyword lists follow its own
/// grammar, not PostgreSQL's quoting rules), sorted for binary search.
const QUOTED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// `format(template, args...)`: PostgreSQL's `%s`, `%I`, `%L` and `%%`
/// specifiers. Positional (`%1$s`) and width forms are refused. A NULL
/// template gives NULL.
#[derive(Debug)]
struct FormatFunction;

impl FormatFunction {
    fn format_row(template: &str, args: &[Option<&str>]) -> Result<String, ExecutionError> {
        let mut out = String::with_capacity(template.len());
        let mut next = 0;
        let mut chars = template.chars();
        while let Some(c) = chars.next() {
            if c != '%' {
                out.push(c);
                continue;
            }
            let Some(spec) = chars.next() else {
                return Err(invalid("unterminated format() type specifier"));
            };
            if spec == '%' {
                out.push('%');
                continue;
            }
            if !matches!(spec, 's' | 'I' | 'L') {
                return Err(invalid(if spec.is_ascii_alphabetic() {
                    format!("unrecognized format() type specifier \"{spec}\"")
                } else {
                    "unsupported format specifier".to_string()
                }));
            }
            let Some(arg) = args.get(next) else {
                return Err(invalid("too few arguments for format()"));
            };
            next += 1;
            match (spec, arg) {
                ('s', arg) => out.push_str(arg.unwrap_or("")),
                ('I', Some(v)) => out.push_str(&quote_ident(v)),
                ('I', None) => {
                    return Err(invalid(
                        "null values cannot be formatted as an SQL identifier",
                    ))
                }
                (_, Some(v)) => {
                    out.push('\'');
                    out.push_str(&v.replace('\'', "''"));
                    out.push('\'');
                }
                (_, None) => out.push_str("NULL"),
            }
        }
        Ok(out)
    }
}

impl ScalarFunction for FormatFunction {
    fn name(&self) -> &str {
        "FORMAT"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        self.invoke(args, 0)
    }
    fn invoke(&self, args: &[ArrayRef], num_rows: usize) -> Result<ArrayRef, ExecutionError> {
        let Some(first) = args.first() else {
            return Err(invalid("FORMAT requires at least 1 argument"));
        };
        let _ = num_rows;
        let template = utf8(first, "FORMAT")?;
        let values: Vec<StringArray> = args[1..]
            .iter()
            .map(|a| utf8(a, "FORMAT"))
            .collect::<Result<_, _>>()?;
        let mut out = StringBuilder::new();
        for row in 0..template.len() {
            if template.is_null(row) {
                out.append_null();
                continue;
            }
            let row_args: Vec<Option<&str>> = values
                .iter()
                .map(|v| (!v.is_null(row)).then(|| v.value(row)))
                .collect();
            out.append_value(Self::format_row(template.value(row), &row_args)?);
        }
        Ok(Arc::new(out.finish()))
    }
}

/// `current_schemas(include_implicit)`: the session's search path as an
/// array, with `pg_catalog` first when `include_implicit` is true (PostgreSQL's
/// order). The planner appends the search path as a second argument (the
/// session's default schema, as `current_schema` reads it); NULL for a NULL
/// argument.
#[derive(Debug)]
struct CurrentSchemasFunction;

impl ScalarFunction for CurrentSchemasFunction {
    fn name(&self) -> &str {
        "CURRENT_SCHEMAS"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::List(Box::new(DataType::Utf8)))
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("CURRENT_SCHEMAS", args, 2, 2)?;
        let include_implicit =
            arrow::compute::cast(&args[0], &ArrowDataType::Boolean).map_err(|e| {
                invalid(format!(
                    "CURRENT_SCHEMAS: the argument must be a boolean: {e}"
                ))
            })?;
        let include_implicit = include_implicit
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("cast to Boolean yields BooleanArray");
        let path = utf8(&args[1], "CURRENT_SCHEMAS")?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for i in 0..include_implicit.len() {
            if include_implicit.is_null(i) || path.is_null(i) {
                builder.append(false);
                continue;
            }
            if include_implicit.value(i) {
                builder.values().append_value("pg_catalog");
            }
            builder.values().append_value(path.value(i));
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// `current_setting(name [, missing_ok])`: the value of a server setting
/// ([`pg_settings::server_setting`], the table `SHOW` reads). An unknown
/// name is PostgreSQL's error, or NULL with `missing_ok`. `search_path` is
/// per session and answered only by `SHOW search_path`.
#[derive(Debug)]
struct CurrentSettingFunction;

impl ScalarFunction for CurrentSettingFunction {
    fn name(&self) -> &str {
        "CURRENT_SETTING"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity("CURRENT_SETTING", args, 1, 2)?;
        let names = utf8(&args[0], "CURRENT_SETTING")?;
        let missing_ok = match args.get(1) {
            Some(arg) => Some(boolean(arg)?),
            None => None,
        };
        let mut out = Vec::with_capacity(names.len());
        for (i, name) in names.iter().enumerate() {
            let Some(name) = name else {
                out.push(None);
                continue;
            };
            let lower = name.to_ascii_lowercase();
            if let Some(value) = pg_settings::server_setting(&lower) {
                out.push(Some(value));
                continue;
            }
            if missing_ok
                .as_ref()
                .is_some_and(|m| m.is_valid(i) && m.value(i))
            {
                out.push(None);
                continue;
            }
            return Err(invalid(if lower == "search_path" {
                "current_setting('search_path') is not supported; use SHOW search_path".to_string()
            } else {
                format!("unrecognized configuration parameter \"{name}\"")
            }));
        }
        Ok(Arc::new(StringArray::from(out)))
    }
}

fn boolean(arg: &ArrayRef) -> Result<BooleanArray, ExecutionError> {
    let cast = arrow::compute::cast(arg, &ArrowDataType::Boolean).map_err(|e| {
        invalid(format!(
            "CURRENT_SETTING: missing_ok must be a boolean: {e}"
        ))
    })?;
    Ok(cast
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("cast to Boolean yields BooleanArray")
        .clone())
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

/// A definition function (`pg_get_constraintdef`, `pg_get_expr`,
/// `pg_get_indexdef`, `pg_get_serial_sequence`): Arneb has no constraints,
/// defaults, indexes or sequences, so the answer is always NULL.
#[derive(Debug)]
struct NullTextFunction {
    name: &'static str,
    min_args: usize,
    max_args: usize,
}

impl ScalarFunction for NullTextFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.name, args, self.min_args, self.max_args)?;
        Ok(new_null_array(&ArrowDataType::Utf8, args[0].len()))
    }
}

/// The relations a `regclass` conversion sees: its session catalog and
/// default schema, and the `(schema, table)` of every relation the
/// statement's catalog lists. The planner passes them as the last three
/// arguments (literals), the way session defaults reach a function.
struct RegclassScope {
    catalog: String,
    default_schema: String,
    /// Oid to `(schema, table)`; PostgreSQL's own system relations are
    /// always there.
    relations: HashMap<i64, (String, String)>,
}

impl RegclassScope {
    fn from_args(name: &str, args: &[ArrayRef]) -> Result<Option<Self>, ExecutionError> {
        check_arity(name, args, 4, 4)?;
        if args[0].is_empty() {
            return Ok(None);
        }
        let text = |i: usize| -> Result<String, ExecutionError> {
            let a = utf8(&args[i], name)?;
            Ok(if a.is_null(0) {
                String::new()
            } else {
                a.value(0).to_string()
            })
        };
        let (catalog, default_schema, list) = (text(1)?, text(2)?, text(3)?);
        let mut relations = HashMap::new();
        for entry in list
            .split(pg_oid::RELATION_ENTRY_SEP)
            .filter(|e| !e.is_empty())
        {
            if let Some((schema, table)) = entry.split_once(pg_oid::RELATION_FIELD_SEP) {
                relations.insert(
                    pg_oid::relation_oid(&catalog, schema, table),
                    (schema.to_string(), table.to_string()),
                );
            }
        }
        for (table, oid) in pg_oid::SYSTEM_RELATIONS {
            relations
                .entry(*oid)
                .or_insert_with(|| (pg_oid::PG_CATALOG.to_string(), table.to_string()));
        }
        Ok(Some(Self {
            catalog,
            default_schema,
            relations,
        }))
    }

    /// PostgreSQL's `regclassout`: the name, qualified unless the schema is
    /// the default one or `pg_catalog`.
    fn name_of(&self, oid: i64) -> String {
        match self.relations.get(&oid) {
            Some((schema, table))
                if *schema == self.default_schema || schema == pg_oid::PG_CATALOG =>
            {
                quote_ident(table)
            }
            Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
            None => oid.to_string(),
        }
    }
}

/// `ident`, double-quoted when PostgreSQL would quote it (`quote_ident`):
/// not a plain lowercase name, or a keyword.
fn quote_ident(ident: &str) -> String {
    let plain = ident
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && ident
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '$')
        && QUOTED_KEYWORDS.binary_search(&ident).is_err();
    if plain {
        ident.to_string()
    } else {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }
}

/// `__regclass_name(oid, catalog, default_schema, relations)`: what
/// `CAST(<regclass> AS TEXT)` plans to. The relation's name, or the number
/// for an oid the catalog does not list.
#[derive(Debug)]
struct RegclassName;

impl ScalarFunction for RegclassName {
    fn name(&self) -> &str {
        "__REGCLASS_NAME"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Utf8)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        let Some(scope) = RegclassScope::from_args("__REGCLASS_NAME", args)? else {
            return Ok(new_null_array(&ArrowDataType::Utf8, 0));
        };
        let oids = int64(&args[0], "__REGCLASS_NAME")?;
        let result: StringArray = oids.iter().map(|o| o.map(|o| scope.name_of(o))).collect();
        Ok(Arc::new(result))
    }
}

/// `__regclass_oid(name, catalog, default_schema, relations)`: what
/// `CAST(<text> AS REGCLASS)` plans to. NULL gives NULL; a name no relation
/// of the catalog has fails as PostgreSQL does.
#[derive(Debug)]
struct RegclassOid;

impl ScalarFunction for RegclassOid {
    fn name(&self) -> &str {
        "__REGCLASS_OID"
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        Ok(DataType::Int64)
    }
    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        let Some(scope) = RegclassScope::from_args("__REGCLASS_OID", args)? else {
            return Ok(new_null_array(&ArrowDataType::Int64, 0));
        };
        let names = utf8(&args[0], "__REGCLASS_OID")?;
        let mut out = Vec::with_capacity(names.len());
        for name in names.iter() {
            out.push(match name {
                None => None,
                Some(name) => {
                    let oid = pg_oid::regclass_oid(name, &scope.catalog, &scope.default_schema)
                        .ok_or_else(|| invalid(format!("invalid name syntax: \"{name}\"")))?;
                    if !scope.relations.contains_key(&oid) {
                        return Err(invalid(format!("relation \"{name}\" does not exist")));
                    }
                    Some(oid)
                }
            });
        }
        Ok(Arc::new(Int64Array::from(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    #[test]
    fn current_setting_reads_server_settings() {
        let names: ArrayRef = Arc::new(StringArray::from(vec![
            Some("default_table_access_method"),
            Some("Server_Version"),
            None,
        ]));
        let out = CurrentSettingFunction.evaluate(&[names]).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "heap");
        assert_eq!(out.value(1), pg_settings::SERVER_VERSION);
        assert!(out.is_null(2));

        let unknown: ArrayRef = Arc::new(StringArray::from(vec!["no_such"]));
        let err = CurrentSettingFunction
            .evaluate(std::slice::from_ref(&unknown))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("unrecognized configuration parameter"),
            "{err}"
        );
        let ok: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let out = CurrentSettingFunction.evaluate(&[unknown, ok]).unwrap();
        assert!(out.is_null(0));

        let search_path: ArrayRef = Arc::new(StringArray::from(vec!["search_path"]));
        let err = CurrentSettingFunction
            .evaluate(&[search_path])
            .unwrap_err()
            .to_string();
        assert!(err.contains("SHOW search_path"), "{err}");
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

    fn fmt(template: &str, args: &[Option<&str>]) -> Result<Option<String>, ExecutionError> {
        let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![template]))];
        for a in args {
            cols.push(Arc::new(StringArray::from(vec![*a])));
        }
        let out = FormatFunction.evaluate(&cols)?;
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        Ok((!out.is_null(0)).then(|| out.value(0).to_string()))
    }

    #[test]
    fn format_specifiers() {
        let ok = |t: &str, a: &[Option<&str>]| fmt(t, a).unwrap().unwrap();
        assert_eq!(ok("%s.%s", &[Some("a"), Some("b")]), "a.b");
        assert_eq!(ok("%I", &[Some("Mixed Case")]), "\"Mixed Case\"");
        assert_eq!(ok("%I", &[Some("abc")]), "abc");
        assert_eq!(ok("%I", &[Some("select")]), "\"select\"");
        assert_eq!(ok("%I", &[Some("a\"b")]), "\"a\"\"b\"");
        assert_eq!(ok("%L", &[Some("it's")]), "'it''s'");
        assert_eq!(ok("%L", &[None]), "NULL");
        assert_eq!(ok("%s", &[None]), "");
        assert_eq!(ok("100%%", &[]), "100%");
        let err = |t: &str, a: &[Option<&str>]| fmt(t, a).unwrap_err().to_string();
        assert!(err("%s", &[]).contains("too few arguments"));
        assert!(err("%I", &[None]).contains("null values cannot be formatted"));
        assert!(err("%1$s", &[Some("a")]).contains("unsupported format specifier"));
        assert!(err("%10s", &[Some("a")]).contains("unsupported format specifier"));
    }

    #[test]
    fn quoted_keywords_are_sorted() {
        assert!(QUOTED_KEYWORDS.windows(2).all(|w| w[0] < w[1]));
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

    fn scope_args(first: ArrayRef) -> Vec<ArrayRef> {
        let n = first.len();
        let col = |v: &str| -> ArrayRef { Arc::new(StringArray::from(vec![v; n])) };
        let relations = format!(
            "default{f}nation{e}staging{f}Raw Data{e}pg_catalog{f}pg_class",
            f = pg_oid::RELATION_FIELD_SEP,
            e = pg_oid::RELATION_ENTRY_SEP
        );
        vec![first, col("file"), col("default"), col(&relations)]
    }

    #[test]
    fn regclass_names_and_oids_round_trip() {
        let nation = pg_oid::relation_oid("file", "default", "nation");
        let raw = pg_oid::relation_oid("file", "staging", "Raw Data");
        let out = RegclassName
            .evaluate(&scope_args(ints(&[
                Some(nation),
                Some(raw),
                Some(1259),
                Some(7),
                None,
            ])))
            .unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "nation");
        assert_eq!(out.value(1), "staging.\"Raw Data\"");
        assert_eq!(out.value(2), "pg_class");
        assert_eq!(out.value(3), "7");
        assert!(out.is_null(4));

        let names: ArrayRef = Arc::new(StringArray::from(vec![
            Some("nation"),
            None,
            Some("pg_type"),
        ]));
        let out = RegclassOid.evaluate(&scope_args(names)).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), nation);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 1247);

        let missing: ArrayRef = Arc::new(StringArray::from(vec!["nope"]));
        let err = RegclassOid.evaluate(&scope_args(missing)).unwrap_err();
        assert!(err.to_string().contains("relation \"nope\" does not exist"));
    }

    #[test]
    fn definition_functions_are_null() {
        let out = NullTextFunction {
            name: "PG_GET_CONSTRAINTDEF",
            min_args: 1,
            max_args: 2,
        }
        .evaluate(&[ints(&[Some(1), Some(2)])])
        .unwrap();
        assert_eq!(out.null_count(), 2);
    }
}
