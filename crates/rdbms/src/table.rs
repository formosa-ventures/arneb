use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// `TableScan.properties` key holding the JSON-encoded [`RemoteTable`].
pub const TABLE_META_PROPERTY: &str = "rdbms.table";

/// A remote table as the catalog discovered it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteTable {
    /// Remote schema (namespace) name, exactly as stored remotely.
    pub schema: String,
    /// Remote table name, exactly as stored remotely.
    pub name: String,
    /// Columns in remote ordinal order; index `i` matches column `i` of the
    /// Arneb table schema.
    pub columns: Vec<RemoteColumn>,
}

/// One remote column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteColumn {
    /// Remote column name, exactly as stored remotely.
    pub name: String,
    /// The remote type's name (e.g. PostgreSQL `udt_name`: `int4`,
    /// `numeric`, `bpchar`). The connector decodes values by it.
    pub remote_type: String,
}

impl RemoteTable {
    /// Store this table in a `TableScan` properties map.
    pub fn write_properties(&self, properties: &mut HashMap<String, String>) {
        let json = serde_json::to_string(self).expect("RemoteTable serializes");
        properties.insert(TABLE_META_PROPERTY.to_string(), json);
    }

    /// Read a table stored by [`write_properties`](Self::write_properties).
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Self, String> {
        let json = properties
            .get(TABLE_META_PROPERTY)
            .ok_or_else(|| format!("missing '{TABLE_META_PROPERTY}' table property"))?;
        serde_json::from_str(json).map_err(|e| format!("invalid '{TABLE_META_PROPERTY}': {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_round_trip() {
        let t = RemoteTable {
            schema: "public".into(),
            name: "Order, \"Items\"".into(),
            columns: vec![RemoteColumn {
                name: "a,b".into(),
                remote_type: "int4".into(),
            }],
        };
        let mut props = HashMap::new();
        t.write_properties(&mut props);
        assert_eq!(RemoteTable::from_properties(&props).unwrap(), t);
    }

    #[test]
    fn missing_property_is_an_error() {
        assert!(RemoteTable::from_properties(&HashMap::new()).is_err());
    }
}
