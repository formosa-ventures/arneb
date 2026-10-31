//! The durable audit log (manage-api D6): one JSON line per audited action
//! in `audit.jsonl` next to the state file, appended with `O_APPEND` and
//! `sync_data` before the caller answers.
//!
//! The newest `max_entries` entries are also kept in memory for reads; the
//! file is the durable copy. Compaction rewrites the file atomically,
//! dropping entries past `retention_days` and beyond `max_entries`.

use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::file::write_atomically;
use crate::tokens::now_ms;
use crate::StateError;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// The audit log's file name, in the directory of the state file.
const AUDIT_FILE: &str = "audit.jsonl";

/// Retention of the audit log (`[audit]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditSettings {
    /// Most entries kept (`1..=1_000_000`).
    pub max_entries: usize,
    /// Days an entry is kept; `0` means no age limit.
    pub retention_days: u32,
}

/// The result of an audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    /// The action succeeded: `"ok"`.
    Ok,
    /// The action failed with this error code: `{"error": "<code>"}`.
    Error(String),
}

/// One audited action, as the caller describes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRecord {
    /// Who ran it.
    pub user: String,
    /// How the user authenticated: `token`, `session` or `basic`.
    pub via: String,
    /// The action name, e.g. `token.issue`.
    pub action: String,
    /// What it acted on (a token id, a worker id, a query id).
    pub target: String,
    /// The action's parameters; never a secret.
    pub parameters: serde_json::Value,
    /// Whether it succeeded.
    pub outcome: AuditOutcome,
}

/// A stored audit entry: a record with its sequence number and time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Increases by one per entry, across restarts.
    pub seq: u64,
    /// When the entry was appended (milliseconds since the Unix epoch).
    pub time_ms: u64,
    /// The audited action.
    #[serde(flatten)]
    pub record: AuditRecord,
}

/// A page request over the audit log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditQuery {
    /// Most entries to return.
    pub limit: usize,
    /// Only entries with a `seq` below this (the previous page's cursor).
    pub before_seq: Option<u64>,
    /// Only entries of this user (exact match).
    pub user: Option<String>,
    /// Only entries of this action (exact match).
    pub action: Option<String>,
}

/// A page of audit entries, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPage {
    /// The matching entries, newest first, at most `limit`.
    pub entries: Vec<AuditEntry>,
    /// The `before_seq` of the next page; `Some` exactly when `truncated`.
    pub next_cursor: Option<u64>,
    /// More entries match beyond this page.
    pub truncated: bool,
}

struct Inner {
    /// The append handle; `None` in memory, or after a failed write until
    /// [`AuditLog::ensure_writable`] reopens it.
    file: Option<File>,
    /// The newest entries, oldest first, at most `max_entries`.
    entries: VecDeque<AuditEntry>,
    next_seq: u64,
    /// Lines in the file, including ones compaction will drop.
    file_lines: usize,
    writable: bool,
}

/// The audit log. Persistent when opened with a path, in memory otherwise.
/// Appends are serialized and blocking (write + `sync_data`).
pub struct AuditLog {
    path: Option<PathBuf>,
    settings: AuditSettings,
    inner: Mutex<Inner>,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_appends: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog")
            .field("path", &self.path)
            .field("settings", &self.settings)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

impl AuditLog {
    /// Opens the log. `None` keeps it in memory. An existing file is
    /// loaded (an unparsable line is skipped with a WARN naming its line
    /// number) and compacted; a missing one is created, mode 0600.
    pub fn open(path: Option<&Path>, settings: AuditSettings) -> Result<AuditLog, StateError> {
        let mut inner = Inner {
            file: None,
            entries: VecDeque::new(),
            next_seq: 1,
            file_lines: 0,
            writable: true,
        };
        if let Some(p) = path {
            let (loaded, max_seq) = load(p)?;
            inner.next_seq = max_seq + 1;
            inner.entries = keep(loaded, settings, now_ms());
            // Startup compaction: drops expired, surplus and torn lines,
            // so the file ends with a newline before the first append.
            if p.exists() {
                write_atomically(p, &render(&inner.entries)?)?;
            }
            inner.file_lines = inner.entries.len();
            inner.file = Some(open_append(p)?);
        }
        Ok(AuditLog {
            path: path.map(Path::to_path_buf),
            settings,
            inner: Mutex::new(inner),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_appends: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The audit log path for the state file `state_path`: `audit.jsonl`
    /// in the same directory.
    pub fn sibling_of(state_path: &Path) -> PathBuf {
        state_path.with_file_name(AUDIT_FILE)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn io_error(&self, source: std::io::Error) -> StateError {
        StateError::Io {
            path: self.path.clone().unwrap_or_default(),
            source,
        }
    }

    fn injected_failure(&self) -> Option<StateError> {
        #[cfg(any(test, feature = "test-hooks"))]
        if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
            return Some(self.io_error(std::io::Error::other("injected audit append failure")));
        }
        None
    }

    /// Appends `record` as the next entry and syncs it to disk. On failure
    /// the log is marked unwritable until [`Self::ensure_writable`]
    /// succeeds. Blocking.
    pub fn append(&self, record: AuditRecord) -> Result<AuditEntry, StateError> {
        let mut inner = self.lock();
        if !inner.writable {
            return Err(StateError::AuditUnwritable(
                "an earlier append failed".into(),
            ));
        }
        let entry = AuditEntry {
            seq: inner.next_seq,
            time_ms: now_ms(),
            record,
        };
        // The seq is used up even when the write fails: a torn write may
        // have left part of it in the file.
        inner.next_seq += 1;
        if let Err(e) = self.write_line(&mut inner, &entry) {
            inner.writable = false;
            inner.file = None;
            return Err(e);
        }
        inner.entries.push_back(entry.clone());
        while inner.entries.len() > self.settings.max_entries {
            inner.entries.pop_front();
        }
        if self.path.is_some() {
            inner.file_lines += 1;
            let slack = (self.settings.max_entries / 10).max(100);
            if inner.file_lines >= self.settings.max_entries.saturating_add(slack) {
                self.compact(&mut inner);
            }
        }
        Ok(entry)
    }

    fn write_line(&self, inner: &mut Inner, entry: &AuditEntry) -> Result<(), StateError> {
        if let Some(e) = self.injected_failure() {
            return Err(e);
        }
        let Some(file) = inner.file.as_mut() else {
            return Ok(()); // in memory
        };
        let mut line = serde_json::to_vec(entry).map_err(|e| self.io_error(e.into()))?;
        line.push(b'\n');
        file.write_all(&line).map_err(|e| self.io_error(e))?;
        file.sync_data().map_err(|e| self.io_error(e))
    }

    /// Rewrites the file with the retained entries and reopens the append
    /// handle (the old one points at the replaced file). A failed rewrite
    /// keeps the old file and is retried on a later append.
    fn compact(&self, inner: &mut Inner) {
        let Some(path) = &self.path else { return };
        let cutoff = cutoff_ms(self.settings, now_ms());
        inner.entries.retain(|e| e.time_ms >= cutoff);
        let written = render(&inner.entries).and_then(|bytes| write_atomically(path, &bytes));
        if let Err(e) = written {
            tracing::warn!(
                target: "arneb::audit",
                path = %path.display(),
                error = %e,
                "audit log compaction failed; keeping the uncompacted file"
            );
            return;
        }
        inner.file_lines = inner.entries.len();
        match open_append(path) {
            Ok(f) => inner.file = Some(f),
            Err(e) => {
                tracing::error!(
                    target: "arneb::audit",
                    path = %path.display(),
                    error = %e,
                    "cannot reopen the audit log after compaction"
                );
                inner.file = None;
                inner.writable = false;
            }
        }
    }

    /// The entries matching `q`, newest first. Entries past
    /// `retention_days` are hidden.
    pub fn query(&self, q: &AuditQuery) -> AuditPage {
        let cutoff = cutoff_ms(self.settings, now_ms());
        let inner = self.lock();
        let mut matching = inner.entries.iter().rev().filter(|e| {
            e.time_ms >= cutoff
                && q.before_seq.is_none_or(|b| e.seq < b)
                && q.user.as_ref().is_none_or(|u| &e.record.user == u)
                && q.action.as_ref().is_none_or(|a| &e.record.action == a)
        });
        let entries: Vec<AuditEntry> = matching.by_ref().take(q.limit).cloned().collect();
        let truncated = matching.next().is_some();
        AuditPage {
            next_cursor: if truncated {
                entries.last().map(|e| e.seq)
            } else {
                None
            },
            entries,
            truncated,
        }
    }

    /// A no-op while writable; otherwise reopens the file, and the log is
    /// writable again when that succeeds.
    pub fn ensure_writable(&self) -> Result<(), StateError> {
        let mut inner = self.lock();
        if inner.writable {
            return Ok(());
        }
        if let Some(e) = self.injected_failure() {
            return Err(e);
        }
        if let Some(path) = &self.path {
            let mut file = open_append(path)?;
            // A torn write may have left a partial line; end it so the next
            // entry starts on its own line (the partial one is skipped at
            // the next load and dropped by compaction).
            if ends_without_newline(path).map_err(|e| self.io_error(e))? {
                file.write_all(b"\n").map_err(|e| self.io_error(e))?;
                file.sync_data().map_err(|e| self.io_error(e))?;
                inner.file_lines += 1;
            }
            inner.file = Some(file);
        }
        inner.writable = true;
        tracing::info!(target: "arneb::audit", "the audit log is writable again");
        Ok(())
    }

    /// Whether the next append is expected to succeed.
    pub fn is_writable(&self) -> bool {
        self.lock().writable
    }

    /// Whether entries are written to disk.
    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    /// Entries held in memory (the newest, at most `max_entries`).
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Test hook: make every append fail (and [`Self::ensure_writable`]
    /// keep failing) while `fail` is set.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_fail_appends(&self, fail: bool) {
        self.fail_appends
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Entries older than this are past retention (`0` = no age limit).
fn cutoff_ms(settings: AuditSettings, now: u64) -> u64 {
    match settings.retention_days {
        0 => 0,
        days => now.saturating_sub(u64::from(days) * DAY_MS),
    }
}

/// The retained entries of `loaded` (in file order): younger than the
/// retention and at most the newest `max_entries`.
fn keep(loaded: Vec<AuditEntry>, settings: AuditSettings, now: u64) -> VecDeque<AuditEntry> {
    let cutoff = cutoff_ms(settings, now);
    let mut kept: VecDeque<AuditEntry> =
        loaded.into_iter().filter(|e| e.time_ms >= cutoff).collect();
    while kept.len() > settings.max_entries {
        kept.pop_front();
    }
    kept
}

/// The parsed lines of `path` and the highest seq among them (`0` when
/// none). A missing file is empty; an unparsable line is skipped.
fn load(path: &Path) -> Result<(Vec<AuditEntry>, u64), StateError> {
    let text = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(source) => {
            return Err(StateError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let mut entries = Vec::new();
    let mut max_seq = 0;
    for (i, line) in text.split(|b| *b == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<AuditEntry>(line) {
            Ok(e) => {
                max_seq = max_seq.max(e.seq);
                entries.push(e);
            }
            Err(e) => tracing::warn!(
                target: "arneb::audit",
                path = %path.display(),
                line = i + 1,
                error = %e,
                "skipping an unreadable audit log line; the next compaction drops it"
            ),
        }
    }
    Ok((entries, max_seq))
}

fn render(entries: &VecDeque<AuditEntry>) -> Result<Vec<u8>, StateError> {
    let mut out = Vec::new();
    for e in entries {
        serde_json::to_writer(&mut out, e)
            .map_err(|e| StateError::AuditUnwritable(e.to_string()))?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Opens `path` for appending, creating it with mode 0600.
fn open_append(path: &Path) -> Result<File, StateError> {
    let mut opts = std::fs::OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path).map_err(|source| StateError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn ends_without_newline(path: &Path) -> std::io::Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = File::open(path)?;
    if f.metadata()?.len() == 0 {
        return Ok(false);
    }
    f.seek(SeekFrom::End(-1))?;
    let mut last = [0u8];
    f.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(max_entries: usize, retention_days: u32) -> AuditSettings {
        AuditSettings {
            max_entries,
            retention_days,
        }
    }

    fn record(user: &str, action: &str) -> AuditRecord {
        AuditRecord {
            user: user.into(),
            via: "basic".into(),
            action: action.into(),
            target: "t".into(),
            parameters: serde_json::json!({}),
            outcome: AuditOutcome::Ok,
        }
    }

    fn line(seq: u64, time_ms: u64) -> String {
        let e = AuditEntry {
            seq,
            time_ms,
            record: record("alice", "token.issue"),
        };
        serde_json::to_string(&e).unwrap()
    }

    fn all(log: &AuditLog) -> Vec<u64> {
        log.query(&AuditQuery {
            limit: usize::MAX,
            ..AuditQuery::default()
        })
        .entries
        .iter()
        .map(|e| e.seq)
        .collect()
    }

    fn lines(p: &Path) -> usize {
        std::fs::read_to_string(p).unwrap().lines().count()
    }

    #[test]
    fn entry_serializes_flat_with_the_outcome_shapes() {
        let mut e = AuditEntry {
            seq: 7,
            time_ms: 9,
            record: record("alice", "token.revoke"),
        };
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"seq": 7, "time_ms": 9, "user": "alice", "via": "basic",
                "action": "token.revoke", "target": "t", "parameters": {}, "outcome": "ok"})
        );
        e.record.outcome = AuditOutcome::Error("not_found".into());
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["outcome"], serde_json::json!({"error": "not_found"}));
        assert_eq!(serde_json::from_value::<AuditEntry>(v).unwrap(), e);
    }

    #[test]
    fn sibling_is_audit_jsonl_next_to_the_state_file() {
        assert_eq!(
            AuditLog::sibling_of(Path::new("/var/lib/arneb/state.json")),
            PathBuf::from("/var/lib/arneb/audit.jsonl")
        );
        assert_eq!(
            AuditLog::sibling_of(Path::new("state.json")),
            PathBuf::from("audit.jsonl")
        );
    }

    #[test]
    fn append_survives_reopen_and_seq_continues() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        for _ in 0..3 {
            log.append(record("alice", "token.issue")).unwrap();
        }
        drop(log);
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        assert!(log.is_persistent());
        assert_eq!(all(&log), vec![3, 2, 1], "newest first");
        assert_eq!(log.append(record("bob", "token.revoke")).unwrap().seq, 4);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&p).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn torn_last_line_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let now = now_ms();
        std::fs::write(
            &p,
            format!("{}\n{}\n{{\"seq\":3,", line(1, now), line(2, now)),
        )
        .unwrap();
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        assert_eq!(all(&log), vec![2, 1]);
        // Compaction (at open) dropped the torn line, so the next append
        // starts on a fresh line.
        assert_eq!(log.append(record("alice", "x")).unwrap().seq, 3);
        drop(log);
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 3);
        for l in text.lines() {
            serde_json::from_str::<AuditEntry>(l).unwrap();
        }
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        assert_eq!(all(&log), vec![3, 2, 1]);
    }

    #[test]
    fn retention_and_max_entries_compact_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        for i in 0..250 {
            log.append(record("alice", "a")).unwrap();
            assert!(lines(&p) <= 200, "after {i}: {} lines", lines(&p));
        }
        assert_eq!(log.len(), 100);
        drop(log);
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        assert_eq!(all(&log), (151..=250).rev().collect::<Vec<_>>());
        assert_eq!(lines(&p), 100);
        drop(log);

        // Age: two days old is past retention_days = 1.
        let now = now_ms();
        let old = now - 2 * DAY_MS;
        std::fs::write(
            &p,
            format!("{}\n{}\n{}\n", line(1, old), line(2, old), line(3, now)),
        )
        .unwrap();
        let log = AuditLog::open(Some(&p), settings(100, 1)).unwrap();
        assert_eq!(all(&log), vec![3]);
        assert_eq!(lines(&p), 1, "compaction at open drops expired entries");
        // seq keeps increasing past the expired entries.
        assert_eq!(log.append(record("alice", "a")).unwrap().seq, 4);
        assert!(!tmp.path().join("audit.jsonl.tmp").exists());
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, vec![std::ffi::OsString::from("audit.jsonl")]);
    }

    #[test]
    fn query_filters_and_pages() {
        let log = AuditLog::open(None, settings(1000, 0)).unwrap();
        for i in 0..120 {
            let user = if i % 2 == 0 { "alice" } else { "bob" };
            let action = if i % 3 == 0 {
                "token.issue"
            } else {
                "token.revoke"
            };
            log.append(record(user, action)).unwrap();
        }
        let page = log.query(&AuditQuery {
            limit: 50,
            ..AuditQuery::default()
        });
        assert_eq!(page.entries.len(), 50);
        assert!(page.truncated);
        assert_eq!(page.next_cursor, Some(page.entries[49].seq));
        assert_eq!(page.entries[0].seq, 120);

        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = log.query(&AuditQuery {
                limit: 50,
                before_seq: cursor,
                ..AuditQuery::default()
            });
            seen.extend(page.entries.iter().map(|e| e.seq));
            assert_eq!(page.truncated, page.next_cursor.is_some());
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(seen, (1..=120).rev().collect::<Vec<_>>());

        let page = log.query(&AuditQuery {
            limit: 500,
            user: Some("bob".into()),
            action: Some("token.issue".into()),
            ..AuditQuery::default()
        });
        assert_eq!(page.entries.len(), 20);
        assert!(!page.truncated);
        assert!(page
            .entries
            .iter()
            .all(|e| e.record.user == "bob" && e.record.action == "token.issue"));
        let page = log.query(&AuditQuery {
            limit: 500,
            user: Some("bo".into()),
            ..AuditQuery::default()
        });
        assert!(page.entries.is_empty(), "the user filter is an exact match");
    }

    #[test]
    fn failed_append_marks_unwritable() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        log.append(record("alice", "a")).unwrap();
        log.set_fail_appends(true);
        assert!(log.append(record("alice", "b")).is_err());
        assert!(!log.is_writable());
        assert!(log.ensure_writable().is_err(), "still failing");
        log.set_fail_appends(false);
        log.ensure_writable().unwrap();
        assert!(log.is_writable());
        log.append(record("alice", "c")).unwrap();
        drop(log);
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        assert_eq!(log.len(), 2, "the failed append is not in the file");
    }

    #[test]
    fn in_memory_log_is_not_persistent() {
        let log = AuditLog::open(None, settings(10, 0)).unwrap();
        assert!(!log.is_persistent());
        assert!(log.is_writable());
        assert!(log.is_empty());
        assert_eq!(log.append(record("alice", "a")).unwrap().seq, 1);
        assert_eq!(log.len(), 1);
    }
}
