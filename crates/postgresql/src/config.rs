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
        // The driver ends the userinfo at the first `@` anywhere, so an
        // unencoded `@` past the authority (in the path, a query value such
        // as `?user=admin@srv` or `?password=Xy@9zK`, or a fragment) turns
        // the rest of the URL into the "user", "password" and "host", with
        // a port or an IPv6 host as much as without. Refuse that before the
        // driver parses it, and never echo any part: each may hold a
        // password.
        if at_outside_authority(&self.connection_url) {
            return Err(AMBIGUOUS_AT.into());
        }
        let mut config = tokio_postgres::Config::from_str(&self.connection_url)
            .map_err(|_| "invalid connection_url".to_string())?;
        // Defense in depth: a split in the wrong place leaves a URL
        // delimiter in the parsed user, password or host. Checked before
        // `self.user` / `self.password` apply (those are not parsed from
        // the URL). A password that really holds one of these characters
        // goes in the `password` field instead.
        let bad_host = config.get_hosts().iter().any(|h| match h {
            tokio_postgres::config::Host::Tcp(h) => h.contains(['=', '&', '?', '/', '@', '#']),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => false,
        });
        let bad_user = config
            .get_user()
            .is_some_and(|u| u.contains(['/', '?', '&', '=', '#']));
        let bad_password = config
            .get_password()
            .is_some_and(|p| p.iter().any(|b| b"/?&=#".contains(b)));
        if bad_host || bad_user {
            return Err(AMBIGUOUS_AT.into());
        }
        if bad_password {
            return Err(
                "invalid connection_url: its password contains '/', '?', '&', '=' or '#'; \
                 set it in the catalog's password field instead"
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

/// The refusal for a URL the driver would split at the wrong `@`.
const AMBIGUOUS_AT: &str = "invalid connection_url: an unencoded '@' splits it in the wrong \
     place; percent-encode an '@' in a user name, password or parameter value as %40";

/// Whether the first `@` of `url` lies past its authority: after the first
/// `/`, `?` or `#` that follows the scheme's `//`. The driver would still
/// end the userinfo there, so where the user, password and host of such a
/// URL end cannot be told apart.
pub fn at_outside_authority(url: &str) -> bool {
    let Some(start) = url.find("://").map(|i| i + 3) else {
        return false;
    };
    let rest = &url[start..];
    match (rest.find('@'), rest.find(['/', '?', '#'])) {
        (Some(at), Some(end)) => at > end,
        _ => false,
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
        // An unencoded `@` anywhere but before the host splits the URL in
        // the wrong place; nothing of the password may show.
        for (url, parts) in [
            (
                "postgresql://db.internal:5432/sales?user=admin@srv&password=S3cret",
                &["S3cret"][..],
            ),
            (
                "postgresql://db/sales?user=admin@srv&password=S3cret",
                &["S3cret"],
            ),
            (
                "postgresql://db.internal/sales?user=app&password=Xy@9zK",
                &["Xy", "9zK"],
            ),
            (
                "postgresql://db/sales?user=admin@srv/x&password=S3cret",
                &["S3cret"],
            ),
            ("postgresql://u:p@ss@host/db", &["ss@"]),
            // A port or an IPv6 host does not let the `@` through.
            (
                "postgresql://db.internal:5432/sales?user=app&password=Xy@9zK",
                &["Xy", "9zK"],
            ),
            ("postgresql://[::1]:5432/db?password=Xy@9zK", &["Xy", "9zK"]),
            ("postgresql://[::1]/db?password=Xy@9zK", &["Xy", "9zK"]),
            ("postgresql://db:5432/sales#x@9zK", &["9zK"]),
            // A delimiter in the userinfo password (raw or decoded).
            ("postgresql://u:p/w@[::1]:5432/db", &["p/w"]),
            ("postgresql://u:p?w@db:5432/db", &["p?w"]),
            ("postgresql://u:a%2Fb%3Dc@db:5432/db", &["a/b", "a%2Fb"]),
            ("postgresql://db:5432/db?password=a%26b", &["a&b", "a%26b"]),
        ] {
            let c = PostgresConfig::new(url);
            let e = c.validate_url().unwrap_err();
            assert_eq!(c.display_target(), "<invalid connection_url>", "{url}");
            for part in parts {
                assert!(
                    !e.contains(part) && !format!("{c:?}").contains(part),
                    "{url}: {e}"
                );
            }
        }
        for url in [
            "postgresql://db/sales?user=admin%40srv&password=x",
            "postgresql://u:pw@[::1]:5432/db",
            "postgresql://u:p%40ss@db.internal:5432/db?sslmode=disable",
            "postgresql://db.internal:5432/sales?user=app&password=Xy%409zK",
            "postgresql://u@db/db?password=x@y",
        ] {
            assert!(
                PostgresConfig::new(url).validate_url().is_ok(),
                "{url} should be accepted"
            );
        }
    }
}
