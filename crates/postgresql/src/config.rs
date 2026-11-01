use std::fmt;
use std::str::FromStr;

/// Default pool size per catalog per node.
pub const DEFAULT_MAX_CONNECTIONS: usize = 8;
/// Default minimum estimated rows per split.
pub const DEFAULT_SPLIT_MIN_ROWS: u64 = 500_000;
/// Default maximum number of splits per table.
pub const DEFAULT_MAX_SPLITS: usize = 16;
/// Default scale for unconstrained `numeric` columns.
pub const DEFAULT_UNCONSTRAINED_NUMERIC_SCALE: i8 = 10;
/// Default limit, in seconds, on opening one connection (TCP + startup).
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
/// Default limit, in seconds, on waiting for a free pooled connection.
pub const DEFAULT_POOL_WAIT_TIMEOUT_SECS: u64 = 60;

/// Connection and scan settings for one PostgreSQL catalog.
#[derive(Clone)]
pub struct PostgresConfig {
    /// `postgresql://host:port/database` (may also carry user/password).
    pub connection_url: String,
    /// User name; overrides one in the URL.
    pub user: Option<String>,
    /// Password; overrides one in the URL. Never logged.
    pub password: Option<String>,
    /// Pool size per node.
    pub max_connections: usize,
    /// Minimum estimated rows per split.
    pub split_min_rows: u64,
    /// Maximum splits per table.
    pub max_splits: usize,
    /// Scale used for unconstrained `numeric` columns.
    pub unconstrained_numeric_scale: i8,
    /// Seconds allowed to open one connection; a `connect_timeout` in the
    /// URL takes precedence for the TCP connect.
    pub connect_timeout_secs: u64,
    /// Seconds a scan or metadata lookup waits for a free pooled connection
    /// before it fails.
    pub pool_wait_timeout_secs: u64,
    /// A dynamic-filter `IN` list with more values than this is pushed as a
    /// `BETWEEN min AND max` range instead.
    pub domain_compaction_threshold: usize,
}

impl PostgresConfig {
    /// A config with defaults for everything but the URL.
    pub fn new(connection_url: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            user: None,
            password: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            split_min_rows: DEFAULT_SPLIT_MIN_ROWS,
            max_splits: DEFAULT_MAX_SPLITS,
            unconstrained_numeric_scale: DEFAULT_UNCONSTRAINED_NUMERIC_SCALE,
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            pool_wait_timeout_secs: DEFAULT_POOL_WAIT_TIMEOUT_SECS,
            domain_compaction_threshold: arneb_rdbms::DEFAULT_DOMAIN_COMPACTION_THRESHOLD,
        }
    }

    /// The driver config, or an error that never contains the password.
    pub(crate) fn driver_config(&self) -> Result<tokio_postgres::Config, String> {
        if !(self.connection_url.starts_with("postgresql://")
            || self.connection_url.starts_with("postgres://"))
        {
            return Err("connection_url must start with postgresql://".into());
        }
        let mut config = tokio_postgres::Config::from_str(&self.connection_url)
            .map_err(|_| "invalid connection_url".to_string())?;
        // The driver ends the credentials at the first `@` anywhere, so an
        // unencoded `@` in a query value (`?user=admin@srv&password=...`)
        // turns the rest of the URL into the "host". Refuse that, without
        // echoing the host (it may hold the password).
        let bad_host = config.get_hosts().iter().any(|h| match h {
            tokio_postgres::config::Host::Tcp(h) => h.contains(['=', '&', '?', '/']),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => false,
        });
        if bad_host {
            return Err(
                "invalid connection_url: its host part holds query parameters; \
                        percent-encode an '@' in a user name or parameter value as %40"
                    .into(),
            );
        }
        if let Some(user) = &self.user {
            config.user(user);
        }
        if let Some(password) = &self.password {
            config.password(password);
        }
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(std::time::Duration::from_secs(self.connect_timeout_secs));
        }
        config.application_name("arneb");
        Ok(config)
    }

    /// Checks that `connection_url` parses as the driver will read it. The
    /// error never contains the password.
    pub fn validate_url(&self) -> Result<(), String> {
        self.driver_config().map(|_| ())
    }

    /// `host:port/database` for logs (no credentials).
    pub fn display_target(&self) -> String {
        match self.driver_config() {
            Ok(c) => {
                let host = c
                    .get_hosts()
                    .first()
                    .map(|h| match h {
                        tokio_postgres::config::Host::Tcp(h) => h.clone(),
                        #[cfg(unix)]
                        tokio_postgres::config::Host::Unix(p) => p.display().to_string(),
                    })
                    .unwrap_or_else(|| "localhost".into());
                let port = c.get_ports().first().copied().unwrap_or(5432);
                format!("{host}:{port}/{}", c.get_dbname().unwrap_or_default())
            }
            Err(_) => "<invalid connection_url>".into(),
        }
    }
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("target", &self.display_target())
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("max_connections", &self.max_connections)
            .field("split_min_rows", &self.split_min_rows)
            .field("max_splits", &self.max_splits)
            .field(
                "unconstrained_numeric_scale",
                &self.unconstrained_numeric_scale,
            )
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("pool_wait_timeout_secs", &self.pool_wait_timeout_secs)
            .field(
                "domain_compaction_threshold",
                &self.domain_compaction_threshold,
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_password_in_field_and_url() {
        let mut c = PostgresConfig::new("postgresql://bob:urlsecret@db.internal:5433/tpch");
        c.password = Some("hunter2".into());
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
        assert!(!dbg.contains("urlsecret"), "{dbg}");
        assert!(dbg.contains("db.internal:5433/tpch"), "{dbg}");
    }

    #[test]
    fn user_and_password_override_the_url() {
        let mut c = PostgresConfig::new("postgresql://bob@h:5433/db");
        c.user = Some("alice".into());
        c.password = Some("pw".into());
        let d = c.driver_config().unwrap();
        assert_eq!(d.get_user(), Some("alice"));
        assert_eq!(d.get_password(), Some(&b"pw"[..]));
    }

    #[test]
    fn connect_timeout_defaults_unless_the_url_sets_one() {
        let c = PostgresConfig::new("postgresql://h:5433/db");
        assert_eq!(
            c.driver_config().unwrap().get_connect_timeout(),
            Some(&std::time::Duration::from_secs(
                DEFAULT_CONNECT_TIMEOUT_SECS
            ))
        );
        let c = PostgresConfig::new("postgresql://h:5433/db?connect_timeout=3");
        assert_eq!(
            c.driver_config().unwrap().get_connect_timeout(),
            Some(&std::time::Duration::from_secs(3))
        );
    }

    #[test]
    fn non_url_connection_strings_are_rejected() {
        assert!(PostgresConfig::new("host=h dbname=d")
            .driver_config()
            .is_err());
        assert!(PostgresConfig::new("mysql://h/d").driver_config().is_err());
        // An unencoded `@` in a query value would make the rest the host.
        for url in [
            "postgresql://db.internal:5432/sales?user=admin@srv&password=S3cret",
            "postgresql://db/sales?user=admin@srv&password=S3cret",
        ] {
            let e = PostgresConfig::new(url).validate_url().unwrap_err();
            assert!(!e.contains("S3cret"), "{e}");
            assert_eq!(
                PostgresConfig::new(url).display_target(),
                "<invalid connection_url>"
            );
        }
        assert!(
            PostgresConfig::new("postgresql://db/sales?user=admin%40srv&password=x")
                .validate_url()
                .is_ok()
        );
        assert!(PostgresConfig::new("postgresql://u:p/w@[::1]:5432/db")
            .validate_url()
            .is_ok());
    }
}
