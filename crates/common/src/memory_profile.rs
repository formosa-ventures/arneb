//! Process-wide diagnostic memory profiling helpers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, OnceLock};

/// Master switch for diagnostic memory profiling (`ARNEB_MEM_PROFILE`,
/// default OFF). `=1` enables read-only peak tracking and task-end logs.
pub fn mem_profile_enabled() -> bool {
    const UNKNOWN: u8 = 0;
    const DISABLED: u8 = 1;
    const ENABLED: u8 = 2;

    static STATE: AtomicU8 = AtomicU8::new(UNKNOWN);
    static INIT: OnceLock<()> = OnceLock::new();

    match STATE.load(Ordering::Relaxed) {
        DISABLED => return false,
        ENABLED => return true,
        _ => {}
    }

    INIT.get_or_init(|| {
        let enabled = std::env::var("ARNEB_MEM_PROFILE")
            .map(|v| v == "1")
            .unwrap_or(false);
        STATE.store(if enabled { ENABLED } else { DISABLED }, Ordering::Relaxed);
        tracing::info!(
            target: "arneb::config",
            mem_profile = enabled,
            "ARNEB_MEM_PROFILE effective value (default off; =1 to enable read-only memory profiling)"
        );
    });

    STATE.load(Ordering::Relaxed) == ENABLED
}

/// Per-label running `current` live bytes plus the `peak` it has reached.
/// The task-end snapshot reports `peak` (the resident peak the operator is
/// responsible for), not `current` — by task-end the batches are released so
/// `current` is ~0 and would never surface in `top_consumers`.
#[derive(Debug, Default, Clone, Copy)]
struct LiveBytes {
    current: u64,
    peak: u64,
}

fn live_bytes() -> &'static Mutex<HashMap<&'static str, LiveBytes>> {
    static LIVE_BYTES: OnceLock<Mutex<HashMap<&'static str, LiveBytes>>> = OnceLock::new();
    LIVE_BYTES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Add `bytes` to the process-global live-byte total for `label`, advancing
/// the per-label peak.
pub fn record_live_alloc(label: &'static str, bytes: u64) {
    if !mem_profile_enabled() || bytes == 0 {
        return;
    }

    let mut totals = live_bytes()
        .lock()
        .expect("live-byte registry mutex poisoned");
    let entry = totals.entry(label).or_default();
    entry.current = entry.current.saturating_add(bytes);
    if entry.current > entry.peak {
        entry.peak = entry.current;
    }
}

/// Subtract `bytes` from the process-global live-byte total for `label`.
pub fn record_live_free(label: &'static str, bytes: u64) {
    if !mem_profile_enabled() || bytes == 0 {
        return;
    }

    let mut totals = live_bytes()
        .lock()
        .expect("live-byte registry mutex poisoned");
    if let Some(entry) = totals.get_mut(label) {
        entry.current = entry.current.saturating_sub(bytes);
    }
}

/// RAII guard for a live-byte allocation.
#[derive(Debug)]
pub struct LiveBytesGuard {
    label: &'static str,
    bytes: u64,
}

impl LiveBytesGuard {
    /// Record a live allocation and return a guard that frees it on drop.
    pub fn new(label: &'static str, bytes: u64) -> Self {
        record_live_alloc(label, bytes);
        Self { label, bytes }
    }
}

impl Drop for LiveBytesGuard {
    fn drop(&mut self) {
        record_live_free(self.label, self.bytes);
        self.bytes = 0;
    }
}

/// Current process-global live-byte totals, sorted by bytes descending.
pub fn live_bytes_snapshot() -> Vec<(&'static str, u64)> {
    if !mem_profile_enabled() {
        return Vec::new();
    }

    let mut snapshot: Vec<(&'static str, u64)> = live_bytes()
        .lock()
        .expect("live-byte registry mutex poisoned")
        .iter()
        .map(|(label, b)| (*label, b.peak))
        .collect();
    snapshot.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    snapshot
}
