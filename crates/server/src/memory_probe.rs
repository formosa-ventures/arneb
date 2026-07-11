//! Worker RSS probe (Phase 3b.6b, 2026-05-21).
//!
//! Reads jemalloc's `stats.resident` (≈ what the kernel's cgroup
//! OOM-killer sees) so the task admission gate can back-pressure when
//! the worker is close to its container quota. Catches the
//! untracked Arrow allocations (Filter / Project / Repartition
//! channels / Flight decode buffers) that the per-operator
//! `MemoryPool` doesn't account for.
//!
//! Polling model: a background tokio task advances jemalloc's epoch
//! (~1 μs) and reads `stats.resident` at a fixed interval (default
//! 100 ms). Admission gate reads the cached atomic — zero per-alloc
//! overhead.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{epoch, stats};

/// Snapshot of the worker's recent jemalloc RSS. Refreshed by a
/// background task; admission gate reads the cached value.
#[derive(Debug)]
pub struct MemoryProbe {
    /// Cached `stats.resident` in bytes. `0` means "no reading yet".
    resident_bytes: AtomicU64,
    /// Threshold above which the admission gate should refuse new
    /// tasks. `0` means "no limit configured".
    admission_threshold_bytes: AtomicU64,
}

impl MemoryProbe {
    /// Construct a probe with the given admission threshold. `0`
    /// disables the gate (probe still polls + reports for telemetry).
    pub fn new(admission_threshold_bytes: u64) -> Self {
        Self {
            resident_bytes: AtomicU64::new(0),
            admission_threshold_bytes: AtomicU64::new(admission_threshold_bytes),
        }
    }

    /// Most recent jemalloc `stats.resident` reading in bytes.
    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// True when the probe has a configured threshold and the latest
    /// RSS reading exceeds it. Callers should refuse new task
    /// admission until the RSS drops below the threshold again.
    pub fn over_threshold(&self) -> bool {
        let threshold = self.admission_threshold_bytes.load(Ordering::Relaxed);
        if threshold == 0 {
            return false;
        }
        self.resident_bytes.load(Ordering::Relaxed) > threshold
    }

    /// Admission threshold in bytes (`0` = disabled).
    pub fn threshold_bytes(&self) -> u64 {
        self.admission_threshold_bytes.load(Ordering::Relaxed)
    }
}

/// Spawn the background probe task. On non-jemalloc targets (msvc)
/// this is a no-op — `MemoryProbe::resident_bytes` stays at 0 and
/// `over_threshold` always returns false.
#[cfg(not(target_env = "msvc"))]
pub fn spawn_probe_task(probe: Arc<MemoryProbe>, refresh: Duration) {
    tokio::spawn(async move {
        let advance = match epoch::mib() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "memory_probe: failed to acquire jemalloc epoch mib; probe disabled");
                return;
            }
        };
        let resident = match stats::resident::mib() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "memory_probe: failed to acquire jemalloc stats.resident mib; probe disabled");
                return;
            }
        };
        loop {
            if advance.advance().is_ok() {
                if let Ok(bytes) = resident.read() {
                    probe.resident_bytes.store(bytes as u64, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(refresh).await;
        }
    });
}

#[cfg(target_env = "msvc")]
pub fn spawn_probe_task(_probe: Arc<MemoryProbe>, _refresh: Duration) {
    // No jemalloc on msvc — probe stays at 0 / disabled.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn over_threshold_when_disabled() {
        let probe = MemoryProbe::new(0);
        assert!(!probe.over_threshold());
    }

    #[test]
    fn over_threshold_compares_resident_vs_cap() {
        let probe = MemoryProbe::new(1_000_000);
        probe.resident_bytes.store(500_000, Ordering::Relaxed);
        assert!(!probe.over_threshold());
        probe.resident_bytes.store(1_500_000, Ordering::Relaxed);
        assert!(probe.over_threshold());
    }
}
