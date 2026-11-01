//! The durable audit log (manage-api D6): JSON lines in
//! `<state stem>.audit.jsonl` next to the state file, appended with
//! `O_APPEND` and `sync_data` before the caller answers.
//!
//! An audited action writes two entries: an intent ([`AuditLog::begin`],
//! outcome `pending`) before it is applied, and a completion
//! ([`AuditLog::complete`], with `intent_seq` naming the intent) after. A
//! completed intent is dropped from memory (and so from the next
//! compaction): reads show one entry per finished action. An intent with no
//! completion at the next start (the process died in between) is reported
//! at WARN on `arneb::audit` and completed as `interrupted`.
//!
//! The newest `max_entries` entries are also kept in memory for reads; the
//! file is the durable copy. Compaction rewrites the file atomically,
//! dropping entries past `retention_days` and beyond `max_entries`, but it
//! always keeps the newest entry, so `seq` never goes back below a used
//! value after a restart (reads still hide that entry once it expires).
//!
//! Locking: appends, reopening and compaction are serialized by a writer
//! lock held across the file I/O. Reads ([`AuditLog::query`],
//! [`AuditLog::len`], [`AuditLog::is_writable`]) take only a short memory
//! lock (or an atomic) that is never held across I/O, so they never wait
//! for an fsync or a compaction and may be called from async code.

use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::file::{sync_parent_dir, write_atomically};
use crate::tokens::now_ms;
use crate::StateError;

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// The audit log's file name suffix, after the state file's stem.
const AUDIT_SUFFIX: &str = ".audit.jsonl";

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
    /// An intent entry: the action is about to be applied, or is being
    /// applied: `"pending"`.
    Pending,
    /// The completion written at startup for an intent that never
    /// completed (the process stopped after the intent was recorded): the
    /// action may or may not have been applied: `"interrupted"`.
    Interrupted,
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
    /// Increases with every entry, across restarts; a failed write, or an
    /// intent dropped once completed, leaves a gap. After a crash, a seq
    /// that appears only in an ERROR line may be reused.
    pub seq: u64,
    /// When the entry was appended (milliseconds since the Unix epoch).
    pub time_ms: u64,
    /// On a completion entry: the `seq` of its intent entry. Absent on an
    /// intent and on a single entry written with [`AuditLog::append`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_seq: Option<u64>,
    /// The audited action.
    #[serde(flatten)]
    pub record: AuditRecord,
}

/// A failed [`AuditLog::append`]: the entry that could not be written (its
/// `seq` is used up) and why.
#[derive(Debug, thiserror::Error)]
#[error("appending audit entry {} failed: {source}", entry.seq)]
pub struct AuditAppendError {
    /// The entry as it would have been written, for the error log.
    pub entry: Box<AuditEntry>,
    /// Why the write failed.
    pub source: StateError,
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

/// The file side, under the writer lock.
struct Writer {
    /// The append handle; `None` in memory, or after a failed write until
    /// [`AuditLog::ensure_writable`] reopens it.
    file: Option<File>,
    next_seq: u64,
    /// Lines in the file, including ones compaction will drop.
    file_lines: usize,
    /// Compact once `file_lines` reaches this.
    compact_at: usize,
}

/// The newest entries, oldest first, at most `max_entries`. Shared with
/// compaction as cheap `Arc` clones, so it is never locked across I/O.
type Entries = VecDeque<Arc<AuditEntry>>;

/// The audit log. Persistent when opened with a path, in memory otherwise.
/// Appends are serialized and blocking (write + `sync_data`); reads do not
/// wait for them.
pub struct AuditLog {
    path: Option<PathBuf>,
    settings: AuditSettings,
    writer: Mutex<Writer>,
    entries: Mutex<Entries>,
    writable: AtomicBool,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_appends: AtomicBool,
    #[cfg(any(test, feature = "test-hooks"))]
    fail_completions: AtomicBool,
    /// `(inside, release)`: the next compaction sends on `inside` once it
    /// holds its snapshot, then waits on `release` before writing.
    #[cfg(test)]
    compact_gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
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
    /// number) and compacted; a missing one is created, mode 0600. An
    /// intent entry with no completion is reported at WARN and completed
    /// as [`AuditOutcome::Interrupted`].
    pub fn open(path: Option<&Path>, settings: AuditSettings) -> Result<AuditLog, StateError> {
        let mut writer = Writer {
            file: None,
            next_seq: 1,
            file_lines: 0,
            compact_at: 0,
        };
        let mut entries = VecDeque::new();
        if let Some(p) = path {
            let loaded = load(p)?;
            writer.next_seq = loaded.max_seq + 1;
            let parsed = loaded.entries.len();
            let recovered = recover(loaded.entries, &mut writer.next_seq, p);
            let kept = keep(recovered.entries, settings, now_ms());
            writer.file_lines = parsed + loaded.bad_lines;
            // Startup compaction, only when it drops something: expired or
            // surplus entries, unreadable lines, or a torn last line (so
            // the first append starts on a fresh line); or when it records
            // interrupted intents. Completed intents alone wait for the
            // next compaction.
            let dropped = kept.len() + recovered.completed < parsed;
            if dropped || recovered.interrupted > 0 || loaded.bad_lines > 0 || loaded.torn_tail {
                write_atomically(p, &render(kept.iter().map(|e| &**e))?)?;
                writer.file_lines = kept.len();
            }
            entries = kept;
            writer.file = Some(open_append(p)?);
        }
        writer.compact_at = settings.max_entries.saturating_add(slack(settings));
        Ok(AuditLog {
            path: path.map(Path::to_path_buf),
            settings,
            writer: Mutex::new(writer),
            entries: Mutex::new(entries),
            writable: AtomicBool::new(true),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_appends: AtomicBool::new(false),
            #[cfg(any(test, feature = "test-hooks"))]
            fail_completions: AtomicBool::new(false),
            #[cfg(test)]
            compact_gate: Mutex::new(None),
        })
    }

    /// The audit log path for the state file `state_path`: its stem plus
    /// `.audit.jsonl`, in the same directory (`prod.json` →
    /// `prod.audit.jsonl`), so state files sharing a directory never share
    /// an audit log.
    pub fn sibling_of(state_path: &Path) -> PathBuf {
        let mut name = state_path
            .file_stem()
            .map(std::ffi::OsStr::to_os_string)
            .unwrap_or_else(|| "state".into());
        name.push(AUDIT_SUFFIX);
        state_path.with_file_name(name)
    }

    fn lock_writer(&self) -> std::sync::MutexGuard<'_, Writer> {
        self.writer.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn lock_entries(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn io_error(&self, source: std::io::Error) -> StateError {
        StateError::Io {
            path: self.path.clone().unwrap_or_default(),
            source,
        }
    }

    fn injected_failure(&self, completion: bool) -> Option<StateError> {
        #[cfg(any(test, feature = "test-hooks"))]
        if self.fail_appends.load(Ordering::SeqCst)
            || (completion && self.fail_completions.load(Ordering::SeqCst))
        {
            return Some(self.io_error(std::io::Error::other("injected audit append failure")));
        }
        let _ = completion;
        None
    }

    /// Appends `record` as one complete entry (no intent) and syncs it to
    /// disk. On failure the log is marked unwritable until
    /// [`Self::ensure_writable`] succeeds. Blocking.
    pub fn append(&self, record: AuditRecord) -> Result<AuditEntry, AuditAppendError> {
        self.write_entry(record, None)
    }

    /// Records the intent of an action about to be applied: `record` with
    /// outcome [`AuditOutcome::Pending`], synced to disk. Apply the action
    /// only when this succeeds, then call [`Self::complete`] with the
    /// returned entry's `seq`. Blocking.
    pub fn begin(&self, mut record: AuditRecord) -> Result<AuditEntry, AuditAppendError> {
        record.outcome = AuditOutcome::Pending;
        self.write_entry(record, None)
    }

    /// Records the completion of the intent `intent_seq` (from
    /// [`Self::begin`]) and drops the intent from memory. Blocking. On
    /// failure the intent stays `pending` (and is completed as
    /// `interrupted` at the next start) and the log is marked unwritable.
    pub fn complete(
        &self,
        intent_seq: u64,
        record: AuditRecord,
    ) -> Result<AuditEntry, AuditAppendError> {
        self.write_entry(record, Some(intent_seq))
    }

    fn write_entry(
        &self,
        record: AuditRecord,
        intent_seq: Option<u64>,
    ) -> Result<AuditEntry, AuditAppendError> {
        let mut w = self.lock_writer();
        let entry = AuditEntry {
            seq: w.next_seq,
            time_ms: now_ms(),
            intent_seq,
            record,
        };
        // The seq is used up even when the write fails: a torn write may
        // have left part of it in the file.
        w.next_seq += 1;
        let written = if self.writable.load(Ordering::SeqCst) {
            self.write_line(&mut w, &entry)
        } else {
            Err(StateError::AuditUnwritable(
                "an earlier append failed".into(),
            ))
        };
        if let Err(source) = written {
            self.writable.store(false, Ordering::SeqCst);
            w.file = None;
            return Err(AuditAppendError {
                entry: Box::new(entry),
                source,
            });
        }
        {
            // Still under the writer lock, so memory order is file order.
            let mut entries = self.lock_entries();
            if let Some(intent) = intent_seq {
                if let Some(at) = entries.iter().rposition(|e| e.seq == intent) {
                    entries.remove(at);
                }
            }
            entries.push_back(Arc::new(entry.clone()));
            while entries.len() > self.settings.max_entries {
                entries.pop_front();
            }
        }
        if self.path.is_some() {
            w.file_lines += 1;
            if w.file_lines >= w.compact_at {
                self.compact(&mut w);
            }
        }
        Ok(entry)
    }

    fn write_line(&self, w: &mut Writer, entry: &AuditEntry) -> Result<(), StateError> {
        if let Some(e) = self.injected_failure(entry.intent_seq.is_some()) {
            return Err(e);
        }
        let Some(file) = w.file.as_mut() else {
            return Ok(()); // in memory
        };
        let mut line = serde_json::to_vec(entry).map_err(|e| self.io_error(e.into()))?;
        line.push(b'\n');
        file.write_all(&line).map_err(|e| self.io_error(e))?;
        file.sync_data().map_err(|e| self.io_error(e))
    }

    /// Rewrites the file with the retained entries and reopens the append
    /// handle (the old one points at the replaced file). Runs under the
    /// writer lock, so no entry is appended meanwhile; the memory lock is
    /// held only to apply retention and take a snapshot of `Arc`s, not
    /// across the rewrite. A failed rewrite keeps the old file and is
    /// retried after another `slack` appends.
    fn compact(&self, w: &mut Writer) {
        let Some(path) = &self.path else { return };
        let slack = slack(self.settings);
        let snapshot: Vec<Arc<AuditEntry>> = {
            let mut entries = self.lock_entries();
            retain_young(&mut entries, self.settings, now_ms());
            entries.iter().cloned().collect()
        };
        #[cfg(test)]
        if let Some((inside, release)) = self
            .compact_gate
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            let _ = inside.send(());
            let _ = release.recv();
        }
        let written =
            render(snapshot.iter().map(|e| &**e)).and_then(|bytes| write_atomically(path, &bytes));
        if let Err(e) = written {
            tracing::warn!(
                target: "arneb::audit",
                path = %path.display(),
                error = %e,
                "audit log compaction failed; keeping the uncompacted file"
            );
            w.compact_at = w.file_lines.saturating_add(slack);
            return;
        }
        w.file_lines = snapshot.len();
        w.compact_at = self.settings.max_entries.saturating_add(slack);
        match open_append(path) {
            Ok(f) => w.file = Some(f),
            Err(e) => {
                tracing::error!(
                    target: "arneb::audit",
                    path = %path.display(),
                    error = %e,
                    "cannot reopen the audit log after compaction"
                );
                w.file = None;
                self.writable.store(false, Ordering::SeqCst);
            }
        }
    }

    /// The entries matching `q`, newest first. Entries past
    /// `retention_days` are hidden. Never waits for an append or a
    /// compaction.
    pub fn query(&self, q: &AuditQuery) -> AuditPage {
        let cutoff = cutoff_ms(self.settings, now_ms());
        let entries = self.lock_entries();
        let mut matching = entries.iter().rev().filter(|e| {
            e.time_ms >= cutoff
                && q.before_seq.is_none_or(|b| e.seq < b)
                && q.user.as_ref().is_none_or(|u| &e.record.user == u)
                && q.action.as_ref().is_none_or(|a| &e.record.action == a)
        });
        let page: Vec<AuditEntry> = matching
            .by_ref()
            .take(q.limit)
            .map(|e| AuditEntry::clone(e))
            .collect();
        let truncated = matching.next().is_some();
        AuditPage {
            next_cursor: if truncated {
                page.last().map(|e| e.seq)
            } else {
                None
            },
            entries: page,
            truncated,
        }
    }

    /// A no-op while writable; otherwise reopens the file, and the log is
    /// writable again when that succeeds. Blocking.
    pub fn ensure_writable(&self) -> Result<(), StateError> {
        if self.writable.load(Ordering::SeqCst) {
            return Ok(());
        }
        let mut w = self.lock_writer();
        if self.writable.load(Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(e) = self.injected_failure(false) {
            return Err(e);
        }
        if let Some(path) = &self.path {
            let file = open_append(path)?;
            // A torn write may have left a partial last line: cut it off so
            // the next entry starts on its own line.
            if let Some(keep) = torn_tail_start(path).map_err(|e| self.io_error(e))? {
                file.set_len(keep).map_err(|e| self.io_error(e))?;
                file.sync_data().map_err(|e| self.io_error(e))?;
            }
            w.file = Some(file);
        }
        self.writable.store(true, Ordering::SeqCst);
        tracing::info!(target: "arneb::audit", "the audit log is writable again");
        Ok(())
    }

    /// Whether the next append is expected to succeed. Lock-free.
    pub fn is_writable(&self) -> bool {
        self.writable.load(Ordering::SeqCst)
    }

    /// Whether entries are written to disk.
    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    /// Entries held in memory (the newest, at most `max_entries`).
    pub fn len(&self) -> usize {
        self.lock_entries().len()
    }

    /// Whether the log holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Test hook: make every append fail (and [`Self::ensure_writable`]
    /// keep failing) while `fail` is set.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_fail_appends(&self, fail: bool) {
        self.fail_appends.store(fail, Ordering::SeqCst);
    }

    /// Test hook: make every completion ([`Self::complete`]) fail while
    /// `fail` is set; intents and single entries are still written.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn set_fail_completions(&self, fail: bool) {
        self.fail_completions.store(fail, Ordering::SeqCst);
    }

    /// Test hook: stop the next compaction once it holds its snapshot;
    /// returns `(inside, release)`.
    #[cfg(test)]
    fn stop_next_compaction(&self) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *self.compact_gate.lock().unwrap() = Some((inside_tx, release_rx));
        (inside_rx, release_tx)
    }
}

/// What [`recover`] made of the loaded entries.
struct Recovered {
    /// The entries to keep, in file order, interrupted completions last.
    entries: Vec<AuditEntry>,
    /// Intents dropped because their completion is in the file.
    completed: usize,
    /// Intents with no completion, now completed as interrupted.
    interrupted: usize,
}

/// Drops the intents of `loaded` that were completed, and completes each
/// other intent as [`AuditOutcome::Interrupted`] (new seqs from
/// `next_seq`), reporting it at WARN.
fn recover(loaded: Vec<AuditEntry>, next_seq: &mut u64, path: &Path) -> Recovered {
    let completed: HashSet<u64> = loaded.iter().filter_map(|e| e.intent_seq).collect();
    let mut out = Recovered {
        entries: Vec::with_capacity(loaded.len()),
        completed: 0,
        interrupted: 0,
    };
    let mut orphans = Vec::new();
    for e in loaded {
        if e.record.outcome == AuditOutcome::Pending && e.intent_seq.is_none() {
            if completed.contains(&e.seq) {
                out.completed += 1;
            } else {
                orphans.push(e);
            }
            continue;
        }
        out.entries.push(e);
    }
    let now = now_ms();
    for intent in orphans {
        tracing::warn!(
            target: "arneb::audit",
            path = %path.display(),
            seq = intent.seq,
            time_ms = intent.time_ms,
            user = %intent.record.user,
            action = %intent.record.action,
            target = %intent.record.target,
            "an audited action was recorded as intent but never completed \
             (the server stopped while applying it); it may or may not have \
             been applied, and is recorded as interrupted"
        );
        out.entries.push(AuditEntry {
            seq: *next_seq,
            time_ms: now,
            intent_seq: Some(intent.seq),
            record: AuditRecord {
                outcome: AuditOutcome::Interrupted,
                ..intent.record
            },
        });
        *next_seq += 1;
        out.interrupted += 1;
    }
    out
}

/// Drops the entries of `entries` past the retention, but always keeps the
/// newest one.
fn retain_young(entries: &mut Entries, settings: AuditSettings, now: u64) {
    let cutoff = cutoff_ms(settings, now);
    if cutoff == 0 {
        return;
    }
    let Some(last) = entries.back().map(|e| e.seq) else {
        return;
    };
    entries.retain(|e| e.time_ms >= cutoff || e.seq == last);
}

/// Entries older than this are past retention (`0` = no age limit).
fn cutoff_ms(settings: AuditSettings, now: u64) -> u64 {
    match settings.retention_days {
        0 => 0,
        days => now.saturating_sub(u64::from(days) * DAY_MS),
    }
}

/// Lines beyond `max_entries` the file may grow before compaction.
fn slack(settings: AuditSettings) -> usize {
    (settings.max_entries / 10).max(100)
}

/// The retained entries of `loaded` (in file order): younger than the
/// retention and at most the newest `max_entries`. The newest entry is
/// always kept, so its `seq` survives a restart.
fn keep(loaded: Vec<AuditEntry>, settings: AuditSettings, now: u64) -> Entries {
    let cutoff = cutoff_ms(settings, now);
    let last = loaded.len().saturating_sub(1);
    let mut kept: Entries = loaded
        .into_iter()
        .enumerate()
        .filter(|(i, e)| e.time_ms >= cutoff || *i == last)
        .map(|(_, e)| Arc::new(e))
        .collect();
    while kept.len() > settings.max_entries {
        kept.pop_front();
    }
    kept
}

/// What [`load`] found in the file.
#[derive(Default)]
struct Loaded {
    entries: Vec<AuditEntry>,
    /// The highest seq among `entries` (`0` when none).
    max_seq: u64,
    /// Unparsable lines, skipped.
    bad_lines: usize,
    /// The file does not end with a newline.
    torn_tail: bool,
}

/// The parsed lines of `path`. A missing file is empty; an unparsable line
/// is skipped with a WARN.
fn load(path: &Path) -> Result<Loaded, StateError> {
    let text = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Loaded::default()),
        Err(source) => {
            return Err(StateError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let mut loaded = Loaded {
        torn_tail: text.last().is_some_and(|b| *b != b'\n'),
        ..Loaded::default()
    };
    for (i, line) in text.split(|b| *b == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<AuditEntry>(line) {
            Ok(e) => {
                loaded.max_seq = loaded.max_seq.max(e.seq);
                loaded.entries.push(e);
            }
            Err(e) => {
                loaded.bad_lines += 1;
                tracing::warn!(
                    target: "arneb::audit",
                    path = %path.display(),
                    line = i + 1,
                    error = %e,
                    "skipping an unreadable audit log line; the next compaction drops it"
                );
            }
        }
    }
    Ok(loaded)
}

fn render<'a>(entries: impl Iterator<Item = &'a AuditEntry>) -> Result<Vec<u8>, StateError> {
    let mut out = Vec::new();
    for e in entries {
        serde_json::to_writer(&mut out, e)
            .map_err(|e| StateError::AuditUnwritable(e.to_string()))?;
        out.push(b'\n');
    }
    Ok(out)
}

/// Opens `path` for appending with mode 0600 (tightening an existing
/// file), creating it and syncing its directory when it is missing.
fn open_append(path: &Path) -> Result<File, StateError> {
    let io = |source| StateError::Io {
        path: path.to_path_buf(),
        source,
    };
    let created = !path.exists();
    let mut opts = std::fs::OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts.open(path).map_err(io)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(io)?;
    }
    // Like the rename in `write_atomically`: the file exists either way, so
    // a directory fsync some filesystems refuse (EINVAL on FUSE, SMB, NFS)
    // only weakens crash durability.
    if created {
        if let Err(e) = sync_parent_dir(path) {
            tracing::warn!(
                target: "arneb::audit",
                path = %path.display(),
                error = %e,
                "audit log was created but syncing its directory failed"
            );
        }
    }
    Ok(file)
}

/// Where a torn last line starts (the length to keep), or `None` when the
/// file is empty or ends with a newline. Scans backwards in chunks.
fn torn_tail_start(path: &Path) -> std::io::Result<Option<u64>> {
    use std::io::{Read, Seek, SeekFrom};
    const CHUNK: u64 = 64 * 1024;
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut end = len;
    let mut buf = vec![0u8; CHUNK as usize];
    while end > 0 {
        let start = end.saturating_sub(CHUNK);
        let n = usize::try_from(end - start).unwrap_or(buf.len());
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(&mut buf[..n])?;
        if let Some(i) = buf[..n].iter().rposition(|b| *b == b'\n') {
            let keep = start + i as u64 + 1;
            return Ok((keep < len).then_some(keep));
        }
        end = start;
    }
    Ok((len > 0).then_some(0))
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
            intent_seq: None,
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
            intent_seq: None,
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
    fn sibling_is_named_after_the_state_file_stem() {
        assert_eq!(
            AuditLog::sibling_of(Path::new("/var/lib/arneb/state.json")),
            PathBuf::from("/var/lib/arneb/state.audit.jsonl")
        );
        assert_eq!(
            AuditLog::sibling_of(Path::new("state.json")),
            PathBuf::from("state.audit.jsonl")
        );
        // Two state files in one directory get two audit logs.
        assert_eq!(
            AuditLog::sibling_of(Path::new("/var/lib/arneb/prod.json")),
            PathBuf::from("/var/lib/arneb/prod.audit.jsonl")
        );
        assert_eq!(
            AuditLog::sibling_of(Path::new("/var/lib/arneb/staging.json")),
            PathBuf::from("/var/lib/arneb/staging.audit.jsonl")
        );
        assert_eq!(
            AuditLog::sibling_of(Path::new("/srv/arneb-state")),
            PathBuf::from("/srv/arneb-state.audit.jsonl")
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
    fn expired_newest_entry_is_kept_hidden_and_seq_continues() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let old = now_ms() - 2 * DAY_MS;
        std::fs::write(
            &p,
            format!("{}\n{}\n{}\n", line(1, old), line(2, old), line(3, old)),
        )
        .unwrap();
        let log = AuditLog::open(Some(&p), settings(100, 1)).unwrap();
        // Before any compaction runs at read time, an expired entry in
        // memory is hidden from reads.
        assert_eq!(all(&log), Vec::<u64>::new());
        assert_eq!(log.len(), 1, "the newest entry is kept");
        assert_eq!(lines(&p), 1);
        drop(log);
        let log = AuditLog::open(Some(&p), settings(100, 1)).unwrap();
        assert_eq!(log.append(record("alice", "a")).unwrap().seq, 4);
        assert_eq!(all(&log), vec![4]);
    }

    #[cfg(unix)]
    #[test]
    fn startup_rewrites_only_when_something_is_dropped() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let now = now_ms();
        let text = format!("{}\n{}\n", line(1, now), line(2, now));
        std::fs::write(&p, &text).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        let ino = std::fs::metadata(&p).unwrap().ino();
        drop(AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        let meta = std::fs::metadata(&p).unwrap();
        assert_eq!(meta.ino(), ino, "nothing to drop: no rewrite");
        assert_eq!(meta.permissions().mode() & 0o777, 0o600, "mode is enforced");
        assert_eq!(std::fs::read_to_string(&p).unwrap(), text);
        // A torn tail counts as something to drop.
        std::fs::write(&p, format!("{text}{{\"seq\":3,")).unwrap();
        let ino = std::fs::metadata(&p).unwrap().ino();
        drop(AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        assert_ne!(std::fs::metadata(&p).unwrap().ino(), ino);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), text);
    }

    #[test]
    fn torn_write_is_cut_off_when_the_log_reopens() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        log.append(record("alice", "a")).unwrap();
        // A write that died halfway, then the failure is noticed.
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(b"{\"seq\":2,\"time").unwrap();
        drop(f);
        log.set_fail_appends(true);
        let err = log.append(record("alice", "b")).unwrap_err();
        assert_eq!(err.entry.seq, 2);
        log.set_fail_appends(false);
        log.ensure_writable().unwrap();
        assert_eq!(log.append(record("alice", "c")).unwrap().seq, 3);
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");
        for l in text.lines() {
            serde_json::from_str::<AuditEntry>(l).unwrap();
        }
    }

    #[test]
    fn concurrent_appends_get_unique_seqs_in_file_order() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = std::sync::Arc::new(AuditLog::open(Some(&p), settings(1000, 0)).unwrap());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let log = std::sync::Arc::clone(&log);
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        log.append(record(&format!("u{t}"), "a")).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let in_file: Vec<u64> = std::fs::read_to_string(&p)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<AuditEntry>(l).unwrap().seq)
            .collect();
        let mut in_memory = all(&log);
        in_memory.reverse();
        assert_eq!(in_file, in_memory, "file order == memory order");
        assert_eq!(in_file, (1..=400).collect::<Vec<_>>());
    }

    #[test]
    fn failed_directory_fsync_on_create_still_opens() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        crate::file::FAIL_DIR_SYNC.with(|f| f.set(true));
        let log = AuditLog::open(Some(&p), settings(100, 0));
        crate::file::FAIL_DIR_SYNC.with(|f| f.set(false));
        let log = log.unwrap();
        assert!(log.is_writable());
        log.append(record("alice", "a")).unwrap();
        assert_eq!(lines(&p), 1);
    }

    /// Runs `f` and returns what it logged.
    fn capture<R>(f: impl FnOnce() -> R) -> (R, String) {
        #[derive(Clone, Default)]
        struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let sink = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let r = tracing::subscriber::with_default(sub, || {
            tracing::callsite::rebuild_interest_cache();
            f()
        });
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        (r, out)
    }

    fn file_entries(p: &Path) -> Vec<AuditEntry> {
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn intent_is_written_before_and_replaced_by_its_completion() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        let intent = log.begin(record("root", "worker.cordon")).unwrap();
        assert_eq!(intent.record.outcome, AuditOutcome::Pending);
        // On disk and visible before the action is applied.
        let on_disk = file_entries(&p);
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].record.outcome, AuditOutcome::Pending);
        assert_eq!(on_disk[0].intent_seq, None);
        assert_eq!(all(&log), vec![intent.seq]);
        let done = log
            .complete(intent.seq, record("root", "worker.cordon"))
            .unwrap();
        assert_eq!(done.intent_seq, Some(intent.seq));
        // Reads show the completion only.
        assert_eq!(all(&log), vec![done.seq]);
        assert_eq!(
            serde_json::to_value(&done).unwrap()["intent_seq"],
            serde_json::json!(intent.seq)
        );
        drop(log);
        let (log, logged) = capture(|| AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        assert_eq!(all(&log), vec![done.seq], "the completed intent is dropped");
        assert!(!logged.contains("never completed"), "{logged}");
        assert_eq!(
            lines(&p),
            2,
            "no startup rewrite for completed intents alone"
        );
    }

    #[test]
    fn an_intent_never_completed_is_reported_once_as_interrupted() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = AuditLog::open(Some(&p), settings(100, 0)).unwrap();
        log.append(record("alice", "token.issue")).unwrap();
        let intent = log.begin(record("root", "worker.cordon")).unwrap();
        // The process dies while applying the action.
        drop(log);
        let (log, logged) = capture(|| AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        assert!(logged.contains("WARN"), "{logged}");
        assert!(logged.contains("never completed"), "{logged}");
        assert!(logged.contains("worker.cordon"), "{logged}");
        let page = log.query(&AuditQuery {
            limit: 10,
            ..AuditQuery::default()
        });
        assert_eq!(page.entries.len(), 2);
        let e = &page.entries[0];
        assert_eq!(e.seq, intent.seq + 1);
        assert_eq!(e.intent_seq, Some(intent.seq));
        assert_eq!(e.record.outcome, AuditOutcome::Interrupted);
        assert_eq!(e.record.user, "root");
        assert_eq!(
            serde_json::to_value(e).unwrap()["outcome"],
            serde_json::json!("interrupted")
        );
        assert_eq!(log.append(record("x", "y")).unwrap().seq, intent.seq + 2);
        drop(log);
        // Recorded, so the next start does not report it again.
        let (_log, logged) = capture(|| AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        assert!(!logged.contains("never completed"), "{logged}");
    }

    #[test]
    fn a_failed_completion_leaves_the_intent_pending() {
        let log = AuditLog::open(None, settings(100, 0)).unwrap();
        let intent = log.begin(record("root", "admission.set")).unwrap();
        log.set_fail_completions(true);
        let err = log
            .complete(intent.seq, record("root", "admission.set"))
            .unwrap_err();
        assert_eq!(err.entry.intent_seq, Some(intent.seq));
        assert!(!log.is_writable());
        assert_eq!(all(&log), vec![intent.seq], "the intent stays, pending");
        // An unwritable log refuses the next intent.
        log.set_fail_completions(false);
        assert!(log.begin(record("root", "admission.set")).is_err());
        log.ensure_writable().unwrap();
        assert!(log.begin(record("root", "admission.set")).is_ok());
    }

    /// Reads never wait for a compaction (or an fsync): they take only the
    /// memory lock, which compaction does not hold while it rewrites.
    #[test]
    fn reads_do_not_wait_for_a_compaction() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("audit.jsonl");
        let log = std::sync::Arc::new(AuditLog::open(Some(&p), settings(100, 0)).unwrap());
        // `compact_at` is 100 + 100 lines: the 200th append compacts.
        for _ in 0..199 {
            log.append(record("alice", "a")).unwrap();
        }
        let (inside, release) = log.stop_next_compaction();
        let appender = std::thread::spawn({
            let log = std::sync::Arc::clone(&log);
            move || log.append(record("alice", "a")).unwrap().seq
        });
        inside.recv().unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn({
            let log = std::sync::Arc::clone(&log);
            move || {
                let page = log.query(&AuditQuery {
                    limit: 1,
                    ..AuditQuery::default()
                });
                let _ = done_tx.send((page.entries[0].seq, log.len(), log.is_writable()));
            }
        });
        let read = done_rx.recv_timeout(std::time::Duration::from_secs(5));
        assert!(!appender.is_finished(), "the compaction is still stopped");
        assert_eq!(
            read.expect("a read waited for the compaction"),
            (200, 100, true)
        );
        release.send(()).unwrap();
        assert_eq!(appender.join().unwrap(), 200);
        assert_eq!(lines(&p), 100);
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
