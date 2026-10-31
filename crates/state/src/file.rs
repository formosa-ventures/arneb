//! The atomic, versioned state file behind [`StateStore`].

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

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
    /// The user already holds the maximum number of live tokens.
    #[error("user {user:?} already has {limit} live tokens")]
    TooManyTokens { user: String, limit: usize },
    /// The system random number generator failed.
    #[error("random number generator failure")]
    Random,
    /// The audit log failed a write and has not been reopened since.
    #[error("the audit log is not writable: {0}")]
    AuditUnwritable(String),
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
pub struct StateStore {
    path: Option<PathBuf>,
    /// The current state as an immutable snapshot. The lock is held only to
    /// clone or swap the `Arc`, so readers (the auth hot paths) never wait
    /// for a write's PBKDF2 or fsync.
    data: RwLock<Arc<StateData>>,
    /// Serializes writers; held across apply + persist + swap.
    writer: Mutex<()>,
}

/// Shows only counts: the state holds verifiers and token hashes, which must
/// not end up in logs.
impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.snapshot();
        f.debug_struct("StateStore")
            .field("path", &self.path)
            .field("users", &data.users.len())
            .field("tokens", &data.tokens.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
thread_local! {
    /// Test hook: make the directory fsync after the rename fail.
    static FAIL_DIR_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
            data: RwLock::new(Arc::new(data)),
            writer: Mutex::new(()),
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

    /// The current state. Never waits for a write in progress: the read
    /// lock is held only to clone the `Arc`.
    pub(crate) fn snapshot(&self) -> Arc<StateData> {
        Arc::clone(&self.data.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Applies `f` to a clone of the state, persists the clone, and only then
    /// swaps it in, so a failed write never changes the in-memory state.
    /// Writers are serialized; readers keep seeing the previous snapshot
    /// until the swap.
    pub(crate) fn mutate<R>(
        &self,
        f: impl FnOnce(&mut StateData) -> Result<R, StateError>,
    ) -> Result<R, StateError> {
        let _writer = self.writer.lock().unwrap_or_else(|p| p.into_inner());
        let mut next = StateData::clone(&self.snapshot());
        let out = f(&mut next)?;
        if let Some(path) = &self.path {
            Self::persist(path, &next)?;
        }
        *self.data.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(next);
        Ok(out)
    }

    fn sync_dir(dir: &Path) -> std::io::Result<()> {
        #[cfg(test)]
        if FAIL_DIR_SYNC.with(|f| f.get()) {
            return Err(std::io::Error::other("injected directory fsync failure"));
        }
        std::fs::File::open(dir).and_then(|d| d.sync_all())
    }

    fn persist(path: &Path, data: &StateData) -> Result<(), StateError> {
        let json = serde_json::to_vec_pretty(data).map_err(|e| StateError::Corrupt {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
        write_atomically(path, &json)
    }
}

/// Writes `bytes` to `<path>.tmp` (mode 0600), fsyncs it, renames it over
/// `path`, then fsyncs the parent directory.
pub(crate) fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), StateError> {
    let io = |source| StateError::Io {
        path: path.to_path_buf(),
        source,
    };
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
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    drop(file);
    std::fs::rename(&tmp, path).map_err(io)?;
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    // The rename is the commit point: the new file is already in place,
    // so a failed directory fsync only weakens crash durability and must
    // not make the caller think the write failed.
    if let Err(e) = StateStore::sync_dir(dir) {
        tracing::warn!(
            target: "arneb::config",
            path = %path.display(),
            error = %e,
            "state file was written but syncing its directory failed"
        );
    }
    Ok(())
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
    fn failed_directory_fsync_after_rename_still_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        FAIL_DIR_SYNC.with(|f| f.set(true));
        let r = store.mutate(|d| {
            d.users.push(alice());
            Ok(())
        });
        FAIL_DIR_SYNC.with(|f| f.set(false));
        r.unwrap();
        assert_eq!(names(&store), vec!["alice"]);
        let reopened = StateStore::open(Some(&p)).unwrap();
        assert_eq!(names(&reopened), vec!["alice"]);
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

    #[test]
    fn readers_do_not_wait_for_a_write_in_progress() {
        use std::sync::mpsc;
        use std::time::Duration;
        let store = Arc::new(StateStore::open(None).unwrap());
        add_alice(&store);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let writer = std::thread::spawn({
            let store = Arc::clone(&store);
            move || {
                store
                    .mutate(|d| {
                        entered_tx.send(()).unwrap();
                        // Stand-in for a slow PBKDF2 / fsync under the writer lock.
                        release_rx.recv().unwrap();
                        d.users.clear();
                        Ok(())
                    })
                    .unwrap();
            }
        });
        entered_rx.recv().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn({
            let store = Arc::clone(&store);
            move || {
                let snap = store.snapshot();
                done_tx.send(snap.users.len()).unwrap();
            }
        });
        let seen = done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("a reader must not block on a write in progress");
        assert_eq!(seen, 1, "readers see the previous snapshot until the swap");
        release_tx.send(()).unwrap();
        writer.join().unwrap();
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
