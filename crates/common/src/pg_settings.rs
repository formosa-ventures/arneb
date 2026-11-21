//! The fixed PostgreSQL settings Arneb reports to pgwire clients, shared by
//! `SHOW` (protocol) and `current_setting()` (execution) so the two agree.
//! Per-session settings (`search_path`) are not here: the protocol layer
//! answers them per connection.

/// The PostgreSQL version Arneb reports (`server_version`).
pub const SERVER_VERSION: &str = "14.0";

/// PostgreSQL's value for a setting clients commonly read, by lowercase
/// name; `None` for a setting Arneb does not report.
pub fn server_setting(name: &str) -> Option<&'static str> {
    Some(match name {
        "server_version" => SERVER_VERSION,
        "server_version_num" => "140000",
        "standard_conforming_strings" | "integer_datetimes" => "on",
        "transaction isolation level" | "default_transaction_isolation" => "read committed",
        "transaction_read_only" | "default_transaction_read_only" | "is_superuser" => "off",
        "client_encoding" | "server_encoding" => "UTF8",
        "datestyle" => "ISO, YMD",
        "intervalstyle" => "postgres",
        "timezone" => "Etc/UTC",
        "max_identifier_length" => "63",
        "lc_collate" | "lc_ctype" => "C",
        "default_table_access_method" => "heap",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_and_unknown_settings() {
        assert_eq!(server_setting("server_version"), Some(SERVER_VERSION));
        assert_eq!(server_setting("default_table_access_method"), Some("heap"));
        assert_eq!(server_setting("search_path"), None);
        assert_eq!(server_setting("no_such_setting"), None);
    }
}
