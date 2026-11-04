use std::fmt;
use std::ops::Range;
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
        // The driver ends a query value at the next `&`, so in
        // `?password=Xy&host=9zK` the tail of a password with a raw `&`
        // becomes a real parameter (here a host). Where such a password
        // ends cannot be told: the password must be the last parameter.
        if query_password_not_last(&self.connection_url) {
            return Err(QUERY_PASSWORD_NOT_LAST.into());
        }
        let mut config = tokio_postgres::Config::from_str(&self.connection_url)
            .map_err(|_| "invalid connection_url".to_string())?;
        // A second `@` inside the authority lands in the host, and any
        // other split in the wrong place leaves a URL delimiter in the
        // parsed host or the user as written. The user is checked as
        // written, before decoding, so a percent-encoded `%2F` or `%3D`
        // is accepted (what the refusal advises); the parsed password
        // needs no check: with the `@` inside the authority it is exactly
        // what the URL wrote, and a decoded `%2F` or `%3D` (base64-style
        // passwords) is deliberate.
        let bad_host = config.get_hosts().iter().any(|h| match h {
            tokio_postgres::config::Host::Tcp(h) => h.contains(['=', '&', '?', '/', '@', '#']),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(_) => false,
        });
        let bad_user = raw_users(&self.connection_url)
            .iter()
            .any(|u| u.contains(['/', '?', '&', '=', '#']));
        if bad_host || bad_user {
            return Err(AMBIGUOUS_AT.into());
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
const AMBIGUOUS_AT: &str = "invalid connection_url: an unencoded '@', '/', '?' or '#' splits \
     it in the wrong place; percent-encode them in a user name, password or parameter value \
     (%40, %2F, %3F, %23), or set the password in the catalog's password field";

/// The refusal for a `password` query parameter followed by another one.
const QUERY_PASSWORD_NOT_LAST: &str = "invalid connection_url: a password parameter must be \
     the last one, with any '&' in it percent-encoded (%26); or set the password in the \
     catalog's password field";

/// One parameter of a URL query, as byte ranges into the URL: the whole
/// `key=value` segment, its key, and its value (as written, not decoded).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParam {
    /// `key=value`, without the separating `&`.
    pub segment: Range<usize>,
    /// The key, before the first `=`.
    pub key: Range<usize>,
    /// The value, up to the next `&` (or the end).
    pub value: Range<usize>,
}

/// The parameters of the query that starts after the first `?` at or after
/// byte `from` of `url`, split the way tokio-postgres splits them: a key
/// runs to the next `=` (an `&` before it is part of the key, which the
/// driver then refuses as unknown), a value to the next `&`, and a single
/// trailing `&` ends the query without starting a parameter. A trailing
/// segment with no `=` (which the driver refuses as unterminated) is not
/// returned. Validation and redaction both read the query through this
/// one scanner, so they cannot disagree on where a value ends.
pub fn query_params_from(url: &str, from: usize) -> Vec<QueryParam> {
    let Some(mut pos) = url
        .get(from..)
        .and_then(|rest| rest.find('?'))
        .map(|q| from + q + 1)
    else {
        return Vec::new();
    };
    let mut params = Vec::new();
    while pos < url.len() {
        let Some(eq) = url[pos..].find('=').map(|i| pos + i) else {
            break;
        };
        let value_end = url[eq + 1..].find('&').map_or(url.len(), |i| eq + 1 + i);
        params.push(QueryParam {
            segment: pos..value_end,
            key: pos..eq,
            value: eq + 1..value_end,
        });
        pos = value_end + 1;
    }
    params
}

/// `key` percent-decoded byte by byte, as the driver decodes keys and
/// values (an invalid escape stays as written).
pub fn percent_decode(key: &str) -> Vec<u8> {
    let bytes = key.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                decoded.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    decoded
}

/// Whether a parameter key is `password` once percent-decoded, as the
/// driver decodes keys (`pass%77ord=` sets the password too).
pub fn is_password_key(key: &str) -> bool {
    percent_decode(key) == b"password"
}

/// Whether a `password` query parameter of `url` is followed by another
/// parameter: a raw `&` in the password would make its tail one. Anything
/// after the password's value but a single trailing `&` counts.
fn query_password_not_last(url: &str) -> bool {
    let Some(start) = url.find("://").map(|i| i + 3) else {
        return false;
    };
    query_params_from(url, start)
        .iter()
        .any(|p| is_password_key(&url[p.key.clone()]) && p.value.end + 1 < url.len())
}

/// The user names `url` sets, as written (not percent-decoded): the
/// userinfo's, up to its first `:`, and every query value whose key is
/// `user` once percent-decoded (`us%65r=` sets the user too).
fn raw_users(url: &str) -> Vec<&str> {
    let mut users = Vec::new();
    let Some(start) = url.find("://").map(|i| i + 3) else {
        return users;
    };
    if let Some(at) = url[start..].find('@') {
        let userinfo = &url[start..start + at];
        users.push(userinfo.split_once(':').map_or(userinfo, |(user, _)| user));
    }
    users.extend(
        query_params_from(url, start)
            .into_iter()
            .filter(|p| percent_decode(&url[p.key.clone()]) == b"user")
            .map(|p| &url[p.value])
            .filter(|u| !u.is_empty()),
    );
    users
}

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
            // A raw delimiter in the userinfo password.
            ("postgresql://u:p/w@[::1]:5432/db", &["p/w"]),
            ("postgresql://u:p?w@db:5432/db", &["p?w"]),
            // A raw `&` in a query password (round 4): its tail would be a
            // real parameter.
            ("postgresql://db/x?password=Xy&host=9zK", &["Xy", "9zK"]),
            (
                "postgresql://db/x?sslmode=disable&password=Xy&dbname=9zK",
                &["Xy", "9zK"],
            ),
            ("postgresql://db/x?pass%77ord=Xy&user=9zK", &["Xy", "9zK"]),
            ("postgresql://db/x?password=Xy&9zK", &["Xy", "9zK"]),
            // Round 5: a percent-encoded `user` key is still a user, and
            // its unencoded delimiters are refused.
            (
                "postgresql://db/x?us%65r=a/b=c&password=S3cret",
                &["S3cret"],
            ),
            ("postgresql://db/x?%75ser=a=b", &["a=b"]),
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
        let d = PostgresConfig::new("postgresql://u:Ab%2Fc%3D@db/x")
            .driver_config()
            .unwrap();
        assert_eq!(d.get_password(), Some(&b"Ab/c="[..]));
        for url in [
            "postgresql://db/sales?user=admin%40srv&password=x",
            "postgresql://u:pw@[::1]:5432/db",
            "postgresql://u:p%40ss@db.internal:5432/db?sslmode=disable",
            "postgresql://db.internal:5432/sales?user=app&password=Xy%409zK",
            "postgresql://u@db/db?password=x@y",
            // Percent-encoded delimiters (base64-style passwords) and raw
            // sub-delims in the userinfo are what the operator meant.
            "postgresql://u:a%2Fb%3Dc@db:5432/db",
            "postgresql://u:Ab%2Bc%2F%3D%3D@db/x",
            "postgresql://u:a&b=c@db/x",
            "postgresql://db:5432/db?password=a%26b",
            "postgresql://u:p%2Fw@[::1]:5432/db",
            // The password as the last parameter, `&` encoded (round 4).
            "postgresql://db/x?sslmode=disable&password=Xy%26host%3D9zK",
            // A user percent-encoded as the refusal advises (round 4).
            "postgresql://dom%2Fsvc:pw@db/x",
            "postgresql://db/x?user=dom%2Fsvc%3Dx&password=pw",
            // Round 5: a single trailing `&` (the driver ignores it), after
            // the password or after any other parameter.
            "postgresql://db/x?sslmode=disable&password=pw&",
            "postgresql://db/x?sslmode=require&",
            "postgresql://db/x?us%65r=app&password=pw",
        ] {
            assert!(
                PostgresConfig::new(url).validate_url().is_ok(),
                "{url} should be accepted"
            );
        }
    }

    /// Round 5: one scanner splits the query the way the driver does; a
    /// single trailing `&` starts no parameter, and an `&` before a key's
    /// `=` belongs to the key.
    #[test]
    fn query_params_split_like_the_driver() {
        let url = "postgresql://db/x?a=1&pass%77ord=p=w&";
        let params = query_params_from(url, 0);
        let keys: Vec<&str> = params.iter().map(|p| &url[p.key.clone()]).collect();
        let values: Vec<&str> = params.iter().map(|p| &url[p.value.clone()]).collect();
        assert_eq!(keys, ["a", "pass%77ord"]);
        assert_eq!(values, ["1", "p=w"]);
        assert_eq!(&url[params[1].segment.clone()], "pass%77ord=p=w");
        let url = "postgresql://db/x?a=1&&b=2";
        let keys: Vec<String> = query_params_from(url, 0)
            .iter()
            .map(|p| url[p.key.clone()].to_owned())
            .collect();
        assert_eq!(keys, ["a", "&b"]);
        assert!(query_params_from("postgresql://db/x", 0).is_empty());
        assert!(query_params_from("postgresql://db/x?novalue", 0).is_empty());
    }
}
