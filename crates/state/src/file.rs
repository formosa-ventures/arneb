//! The atomic, versioned state file behind [`StateStore`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// On-disk format version this build reads and writes.
const STATE_VERSION: u32 = 1;

/// Errors from opening or mutating the state store.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Reading or writing the state file failed.
    #[error("state file {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// The state file exists but is not valid state JSON.
    #[error("state file {} is corrupt: {message}", path.display())]
    Corrupt { path: PathBuf, message: String },
    /// The state file was written by a newer, incompatible version.
    #[error("state file {} has unsupported version {found}", path.display())]
    UnsupportedVersion { path: PathBuf, found: u32 },
    /// No user with this name exists.
    #[error("unknown user {0:?}")]
    UnknownUser(String),
    /// No token with this id exists.
    #[error("token {0:?} not found")]
    TokenNotFound(String),
    /// The system random number generator failed.
    #[error("random number generator failure")]
    Random,
}

/// A stored user with a SCRAM-SHA-256 verifier (never a plaintext password).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UserRecord {
    pub name: String,
    pub scram: String,
    /// True when the user was seeded from the config file.
    pub from_config: bool,
}

/// A stored API token. Holds only a hash of the secret and a SCRAM verifier
/// of the full token, never the token itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TokenRecord {
    pub id: String,
    pub user: String,
    pub name: Option<String>,
    pub created_ms: u64,
    pub expires_ms: Option<u64>,
    /// SHA-256 of the secret segment, for constant-time lookup checks.
    pub secret_sha256: [u8; 32],
    /// `ScramVerifier` (Display form) of the full token, sharing the user's
    /// salt and iteration count.
    pub scram: String,
}

/// Everything persisted in the state file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StateData {
    pub version: u32,
    #[serde(default)]
    pub users: Vec<UserRecord>,
    #[serde(default)]
    pub tokens: Vec<TokenRecord>,
}

impl Default for StateData {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            users: Vec::new(),
            tokens: Vec::new(),
        }
    }
}

/// Durable coordinator state. Persistent when opened with a path, in-memory
/// otherwise. Share as `Arc<StateStore>`.
#[derive(Debug)]
pub struct StateStore {
    path: Option<PathBuf>,
    pub(crate) data: Mutex<StateData>,
}

impl StateStore {
    /// Opens the store. `None` keeps state in memory only. A missing file
    /// starts empty and is created on the first write; a leftover
    /// `<path>.tmp` from an interrupted write is ignored.
    pub fn open(path: Option<&Path>) -> Result<StateStore, StateError> {
        let data = match path {
            Some(p) => Self::load(p)?,
            None => StateData::default(),
        };
        Ok(StateStore {
            path: path.map(Path::to_path_buf),
            data: Mutex::new(data),
        })
    }

    /// Whether mutations are written to disk.
    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    /// The state file path, if persistent.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn load(path: &Path) -> Result<StateData, StateError> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(StateData::default()),
            Err(source) => {
                return Err(StateError::Io {
                    path: path.to_path_buf(),
                    source,
                })
            }
        };
        let corrupt = |message: String| StateError::Corrupt {
            path: path.to_path_buf(),
            message,
        };
        // Check the version first so a newer format with extra or changed
        // fields is reported as unsupported rather than corrupt.
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| corrupt(e.to_string()))?;
        let found = value
            .get("version")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| corrupt("missing numeric \"version\"".into()))?;
        if found != u64::from(STATE_VERSION) {
            return Err(StateError::UnsupportedVersion {
                path: path.to_path_buf(),
                found: u32::try_from(found).unwrap_or(u32::MAX),
            });
        }
        serde_json::from_value(value).map_err(|e| corrupt(e.to_string()))
    }

    /// Applies `f` to a clone of the state, persists the clone, and only then
    /// swaps it in, so a failed write never changes the in-memory state.
    pub(crate) fn mutate<R>(
        &self,
        f: impl FnOnce(&mut StateData) -> Result<R, StateError>,
    ) -> Result<R, StateError> {
        let mut guard = self.data.lock().unwrap_or_else(|p| p.into_inner());
        let mut next = guard.clone();
        let out = f(&mut next)?;
        if let Some(path) = &self.path {
            Self::persist(path, &next)?;
        }
        *guard = next;
        Ok(out)
    }

    /// Writes `<path>.tmp` (mode 0600), fsyncs it, renames it over `path`,
    /// then fsyncs the parent directory.
    fn persist(path: &Path, data: &StateData) -> Result<(), StateError> {
        let io = |source| StateError::Io {
            path: path.to_path_buf(),
            source,
        };
        let json = serde_json::to_vec_pretty(data).map_err(|e| StateError::Corrupt {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(".tmp");
        let tmp = PathBuf::from(tmp_name);

        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp).map_err(io)?;
        // `mode` only applies to a newly created file; tighten a reused
        // leftover temp file too.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(io)?;
        }
        file.write_all(&json).map_err(io)?;
        file.sync_all().map_err(io)?;
        drop(file);
        std::fs::rename(&tmp, path).map_err(io)?;
        let dir = match path.parent() {
            Some(d) if !d.as_os_str().is_empty() => d,
            _ => Path::new("."),
        };
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn alice() -> UserRecord {
        UserRecord {
            name: "alice".into(),
            scram: "SCRAM-SHA-256$4096:salt$stored:server".into(),
            from_config: false,
        }
    }

    fn names(store: &StateStore) -> Vec<String> {
        store
            .mutate(|d| Ok(d.users.iter().map(|u| u.name.clone()).collect()))
            .unwrap()
    }

    fn add_alice(store: &StateStore) {
        store
            .mutate(|d| {
                d.users.push(alice());
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn open_missing_file_starts_empty_and_creates_nothing_until_first_write() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert!(store.is_persistent());
        assert_eq!(store.path(), Some(p.as_path()));
        assert!(!p.exists());
        assert!(names(&store).is_empty());
    }

    #[test]
    fn data_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        add_alice(&store);
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(names(&store), vec!["alice"]);
    }

    #[test]
    fn leftover_temp_file_does_not_block_open() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        add_alice(&store);
        drop(store);
        std::fs::write(tmp.path().join("state.json.tmp"), "garbage{").unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(names(&store), vec!["alice"]);
        add_alice(&store); // a later write replaces the stale temp file
    }

    #[test]
    fn corrupt_file_fails_naming_the_path() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, "{not json").unwrap();
        match StateStore::open(Some(&p)) {
            Err(StateError::Corrupt { path, .. }) => assert_eq!(path, p),
            other => panic!("expected Corrupt, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn newer_version_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, r#"{"version":2,"users":[]}"#).unwrap();
        match StateStore::open(Some(&p)) {
            Err(StateError::UnsupportedVersion { found, path }) => {
                assert_eq!(found, 2);
                assert_eq!(path, p);
            }
            other => panic!("expected UnsupportedVersion, got {:?}", other.map(|_| ())),
        }
    }

    #[cfg(unix)]
    #[test]
    fn failed_persist_leaves_memory_unchanged() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("sub");
        std::fs::create_dir(&dir).unwrap();
        let p = dir.join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let r = store.mutate(|d| {
            d.users.push(alice());
            Ok(())
        });
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(r, Err(StateError::Io { .. })));
        assert!(names(&store).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn leftover_loose_temp_file_is_tightened() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let t = tmp.path().join("state.json.tmp");
        std::fs::write(&t, "x").unwrap();
        std::fs::set_permissions(&t, std::fs::Permissions::from_mode(0o644)).unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        add_alice(&store);
        let mode = std::fs::metadata(&p).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn state_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        add_alice(&store);
        let mode = std::fs::metadata(Path::new(&p))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
