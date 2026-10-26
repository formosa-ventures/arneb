//! Object identifiers for the PostgreSQL system catalog that pgwire clients
//! read (pg-catalog-tables).
//!
//! The `pg_catalog` tables and `'name'::regclass` must agree on every oid,
//! so both take them from here. System relations keep their PostgreSQL
//! oids; every other schema and relation gets a stable hash of its
//! qualified name in PostgreSQL's user-oid range, so it never collides
//! with a type or system oid.

/// Oid of the `pg_catalog` namespace, as in PostgreSQL.
pub const PG_CATALOG_NAMESPACE_OID: i64 = 11;

/// Oid of the role that owns every object (`pg_roles`, `relowner`).
pub const OWNER_ROLE_OID: i64 = 10;

/// Name of the role that owns every object.
pub const OWNER_ROLE_NAME: &str = "arneb";

/// Name of the system catalog schema.
pub const PG_CATALOG: &str = "pg_catalog";

/// PostgreSQL system relations and their oids. Includes relations Arneb
/// does not provide (`pg_rewrite`, `pg_index`, …) so that clients' `regclass`
/// literals for them resolve.
pub const SYSTEM_RELATIONS: &[(&str, i64)] = &[
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_proc", 1255),
    ("pg_class", 1259),
    ("pg_attrdef", 2604),
    ("pg_constraint", 2606),
    ("pg_inherits", 2611),
    ("pg_index", 2610),
    ("pg_operator", 2617),
    ("pg_opclass", 2616),
    ("pg_am", 2601),
    ("pg_rewrite", 2618),
    ("pg_trigger", 2620),
    ("pg_namespace", 2615),
    ("pg_description", 2609),
    ("pg_depend", 2608),
    ("pg_database", 1262),
    ("pg_tablespace", 1213),
    ("pg_authid", 1260),
    ("pg_roles", 12001),
    ("pg_collation", 3456),
    ("pg_sequence", 2224),
];

/// The PostgreSQL oid of system relation `name`, if it is one.
pub fn system_relation_oid(name: &str) -> Option<i64> {
    SYSTEM_RELATIONS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, oid)| *oid)
}

/// First oid PostgreSQL assigns to user objects.
const FIRST_USER_OID: i64 = 16_384;

/// A stable oid for an object identified by `parts` (FNV-1a over the
/// parts, folded into `16384..2^31`).
fn hashed_oid(parts: &[&str]) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            hash ^= u64::from(b'.');
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        for byte in part.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    let span = (i32::MAX as u64) - FIRST_USER_OID as u64;
    FIRST_USER_OID + (hash % span) as i64
}

/// Oid of schema `schema` of catalog (PostgreSQL database) `catalog`.
pub fn namespace_oid(catalog: &str, schema: &str) -> i64 {
    if schema == PG_CATALOG {
        PG_CATALOG_NAMESPACE_OID
    } else {
        hashed_oid(&[catalog, schema])
    }
}

/// Oid of relation `catalog.schema.table`. A relation of `pg_catalog` that
/// PostgreSQL defines keeps its PostgreSQL oid.
pub fn relation_oid(catalog: &str, schema: &str, table: &str) -> i64 {
    if schema == PG_CATALOG {
        if let Some(oid) = system_relation_oid(table) {
            return oid;
        }
    }
    hashed_oid(&[catalog, schema, table])
}

/// Oid of a catalog as a PostgreSQL database.
pub fn database_oid(catalog: &str) -> i64 {
    hashed_oid(&[catalog])
}

/// The oid a `'<name>'::regclass` literal denotes, given the session's
/// catalog and default schema. Unquoted parts are lowercased, quoted
/// parts kept as written. An unqualified system relation name resolves to
/// its PostgreSQL oid; any other unqualified name to a table of the
/// default schema. `None` when `name` is not a 1- to 3-part name.
///
/// As in PostgreSQL the result is an oid, not a check: unlike PostgreSQL
/// the relation does not have to exist.
pub fn regclass_oid(name: &str, catalog: &str, default_schema: &str) -> Option<i64> {
    let parts = split_name(name)?;
    match parts.as_slice() {
        [table] => Some(match system_relation_oid(table) {
            Some(oid) => oid,
            None => relation_oid(catalog, default_schema, table),
        }),
        [schema, table] => Some(relation_oid(catalog, schema, table)),
        [database, schema, table] => Some(relation_oid(database, schema, table)),
        _ => None,
    }
}

/// Split a possibly quoted, dotted name (`"Sales".orders`) into parts.
fn split_name(name: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = name.trim().chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    '"' => break,
                    c => part.push(c),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == '.' {
                    break;
                }
                part.extend(c.to_lowercase());
                chars.next();
            }
            part = part.trim().to_string();
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') => {}
            Some(_) => return None,
        }
    }
}

/// A PostgreSQL built-in type as `pg_type` and `format_type` report it.
#[derive(Debug, Clone, Copy)]
pub struct PgType {
    /// `pg_type.oid`.
    pub oid: i64,
    /// `pg_type.typname`.
    pub name: &'static str,
    /// The SQL name `format_type` prints.
    pub sql_name: &'static str,
    /// `pg_type.typlen` (-1 for variable length).
    pub len: i64,
    /// `pg_type.typarray`: the oid of the array type over this one.
    pub array_oid: i64,
    /// `pg_type.typcategory`.
    pub category: &'static str,
}

/// The built-in types Arneb maps its columns to, plus the ones catalog
/// columns use (`name`, `oid`, `char`).
pub const PG_TYPES: &[PgType] = &[
    PgType {
        oid: 16,
        name: "bool",
        sql_name: "boolean",
        len: 1,
        array_oid: 1000,
        category: "B",
    },
    PgType {
        oid: 17,
        name: "bytea",
        sql_name: "bytea",
        len: -1,
        array_oid: 1001,
        category: "U",
    },
    PgType {
        oid: 18,
        name: "char",
        sql_name: "\"char\"",
        len: 1,
        array_oid: 1002,
        category: "Z",
    },
    PgType {
        oid: 19,
        name: "name",
        sql_name: "name",
        len: 64,
        array_oid: 1003,
        category: "S",
    },
    PgType {
        oid: 20,
        name: "int8",
        sql_name: "bigint",
        len: 8,
        array_oid: 1016,
        category: "N",
    },
    PgType {
        oid: 21,
        name: "int2",
        sql_name: "smallint",
        len: 2,
        array_oid: 1005,
        category: "N",
    },
    PgType {
        oid: 23,
        name: "int4",
        sql_name: "integer",
        len: 4,
        array_oid: 1007,
        category: "N",
    },
    PgType {
        oid: 25,
        name: "text",
        sql_name: "text",
        len: -1,
        array_oid: 1009,
        category: "S",
    },
    PgType {
        oid: 26,
        name: "oid",
        sql_name: "oid",
        len: 4,
        array_oid: 1028,
        category: "N",
    },
    PgType {
        oid: 700,
        name: "float4",
        sql_name: "real",
        len: 4,
        array_oid: 1021,
        category: "N",
    },
    PgType {
        oid: 701,
        name: "float8",
        sql_name: "double precision",
        len: 8,
        array_oid: 1022,
        category: "N",
    },
    PgType {
        oid: 1043,
        name: "varchar",
        sql_name: "character varying",
        len: -1,
        array_oid: 1015,
        category: "S",
    },
    PgType {
        oid: 1082,
        name: "date",
        sql_name: "date",
        len: 4,
        array_oid: 1182,
        category: "D",
    },
    PgType {
        oid: 1083,
        name: "time",
        sql_name: "time without time zone",
        len: 8,
        array_oid: 1183,
        category: "D",
    },
    PgType {
        oid: 1114,
        name: "timestamp",
        sql_name: "timestamp without time zone",
        len: 8,
        array_oid: 1115,
        category: "D",
    },
    PgType {
        oid: 1184,
        name: "timestamptz",
        sql_name: "timestamp with time zone",
        len: 8,
        array_oid: 1185,
        category: "D",
    },
    PgType {
        oid: 1700,
        name: "numeric",
        sql_name: "numeric",
        len: -1,
        array_oid: 1231,
        category: "N",
    },
];

/// `format_type(type_oid, typmod)`: the SQL name of a type, with the
/// precision and scale of a `numeric` or the length of a `varchar` taken
/// from `typmod`, and `???` for an unknown oid, as in PostgreSQL.
pub fn format_type(oid: i64, typmod: Option<i64>) -> String {
    let Some(ty) = PG_TYPES.iter().find(|t| t.oid == oid) else {
        return "???".to_string();
    };
    match (ty.oid, typmod) {
        (1700, Some(m)) if m >= 4 => {
            let m = m - 4;
            format!("numeric({},{})", (m >> 16) & 0xffff, m & 0xffff)
        }
        (1043, Some(m)) if m >= 4 => format!("character varying({})", m - 4),
        _ => ty.sql_name.to_string(),
    }
}

/// The oid of the built-in type named `name`, by `typname` (`int4`) or
/// SQL name (`integer`), optionally `pg_catalog.`-qualified: what
/// `to_regtype` and a `'name'::regtype` literal return.
pub fn type_oid(name: &str) -> Option<i64> {
    let name = name.trim().trim_matches('"').to_ascii_lowercase();
    let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    PG_TYPES
        .iter()
        .find(|t| t.name == name || t.sql_name == name)
        .map(|t| t.oid)
}

/// The `atttypmod` PostgreSQL stores for `numeric(precision, scale)`.
pub fn numeric_typmod(precision: u8, scale: i8) -> i64 {
    ((i64::from(precision) << 16) | i64::from(scale.max(0) as u8)) + 4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_relations_keep_postgres_oids() {
        assert_eq!(regclass_oid("pg_rewrite", "file", "default"), Some(2618));
        assert_eq!(
            regclass_oid("pg_catalog.pg_constraint", "file", "default"),
            Some(2606)
        );
        assert_eq!(relation_oid("file", "pg_catalog", "pg_class"), 1259);
        assert_eq!(namespace_oid("file", "pg_catalog"), 11);
    }

    #[test]
    fn user_relations_hash_into_the_user_range() {
        let oid = relation_oid("file", "default", "nation");
        assert!((FIRST_USER_OID..i64::from(i32::MAX)).contains(&oid));
        assert_eq!(oid, relation_oid("file", "default", "nation"));
        assert_ne!(oid, relation_oid("file", "default", "region"));
        assert_ne!(oid, relation_oid("lake", "default", "nation"));
        assert_ne!(
            namespace_oid("file", "default"),
            namespace_oid("lake", "default")
        );
    }

    #[test]
    fn format_type_matches_postgres() {
        assert_eq!(format_type(23, Some(-1)), "integer");
        assert_eq!(
            format_type(1700, Some(numeric_typmod(12, 2))),
            "numeric(12,2)"
        );
        assert_eq!(format_type(1700, Some(-1)), "numeric");
        assert_eq!(format_type(1043, Some(29)), "character varying(25)");
        assert_eq!(format_type(1114, None), "timestamp without time zone");
        assert_eq!(format_type(99999, Some(-1)), "???");
    }

    #[test]
    fn type_names_resolve_to_oids() {
        assert_eq!(type_oid("int4"), Some(23));
        assert_eq!(type_oid("integer"), Some(23));
        assert_eq!(type_oid("pg_catalog.varchar"), Some(1043));
        assert_eq!(type_oid("hstore"), None);
    }

    #[test]
    fn regclass_names_resolve_like_postgres() {
        let nation = relation_oid("file", "default", "nation");
        assert_eq!(regclass_oid("nation", "file", "default"), Some(nation));
        assert_eq!(regclass_oid("NATION", "file", "default"), Some(nation));
        assert_eq!(
            regclass_oid("\"default\".nation", "file", "x"),
            Some(nation)
        );
        assert_eq!(
            regclass_oid("file.default.nation", "other", "x"),
            Some(nation)
        );
        assert_ne!(regclass_oid("\"Nation\"", "file", "default"), Some(nation));
        assert_eq!(regclass_oid("a..b", "file", "default"), None);
        assert_eq!(regclass_oid("\"open", "file", "default"), None);
        assert_eq!(regclass_oid("a.b.c.d", "file", "default"), None);
    }
}
