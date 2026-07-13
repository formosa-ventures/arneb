//! Gated, read-only memory profiling wrapper for worker pools.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arneb_common::memory_profile::live_bytes_snapshot;
use arneb_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryProfileSnapshot};

pub use arneb_common::memory_profile::mem_profile_enabled;

#[derive(Debug, Default)]
struct ConsumerProfile {
    current: u64,
    peak: u64,
}

/// Read-only profiling decorator. It never changes admission/spill decisions;
/// all grow/shrink behavior is delegated to `inner`.
#[derive(Debug)]
pub struct MemoryProfilePool {
    inner: Arc<dyn MemoryPool>,
    current_reserved: AtomicU64,
    peak_reserved: AtomicU64,
    jemalloc_resident_peak: AtomicU64,
    jemalloc_allocated_peak: AtomicU64,
    jemalloc_active_peak: AtomicU64,
    jemalloc_retained_peak: AtomicU64,
    consumers: Mutex<HashMap<String, ConsumerProfile>>,
}

impl MemoryProfilePool {
    /// Wrap `inner` with read-only memory profile tracking.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            current_reserved: AtomicU64::new(0),
            peak_reserved: AtomicU64::new(0),
            jemalloc_resident_peak: AtomicU64::new(0),
            jemalloc_allocated_peak: AtomicU64::new(0),
            jemalloc_active_peak: AtomicU64::new(0),
            jemalloc_retained_peak: AtomicU64::new(0),
            consumers: Mutex::new(HashMap::new()),
        }
    }

    fn update_peak(peak: &AtomicU64, value: u64) {
        let mut observed = peak.load(Ordering::Relaxed);
        while value > observed {
            match peak.compare_exchange_weak(observed, value, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(next) => observed = next,
            }
        }
    }

    fn sample_jemalloc_resident(&self) {
        if let Some(stats) = jemalloc_memory_stats() {
            Self::update_peak(&self.jemalloc_resident_peak, stats.resident);
            Self::update_peak(&self.jemalloc_allocated_peak, stats.allocated);
            Self::update_peak(&self.jemalloc_active_peak, stats.active);
            Self::update_peak(&self.jemalloc_retained_peak, stats.retained);
        }
    }
}

impl MemoryPool for MemoryProfilePool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn try_grow(
        &self,
        consumer: &MemoryConsumer,
        additional: usize,
    ) -> Result<(), arneb_common::error::ExecutionError> {
        self.inner.try_grow(consumer, additional)?;

        let additional = additional as u64;
        let new_total = self
            .current_reserved
            .fetch_add(additional, Ordering::Relaxed)
            .saturating_add(additional);
        Self::update_peak(&self.peak_reserved, new_total);

        let mut consumers = self.consumers.lock().expect("memory pool mutex poisoned");
        let profile = consumers.entry(consumer.name().to_string()).or_default();
        profile.current = profile.current.saturating_add(additional);
        profile.peak = profile.peak.max(profile.current);
        drop(consumers);

        self.sample_jemalloc_resident();
        Ok(())
    }

    fn shrink(&self, consumer: &MemoryConsumer, bytes: usize) {
        self.inner.shrink(consumer, bytes);

        let bytes = bytes as u64;
        let mut current = self.current_reserved.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.current_reserved.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        let mut consumers = self.consumers.lock().expect("memory pool mutex poisoned");
        if let Some(profile) = consumers.get_mut(consumer.name()) {
            profile.current = profile.current.saturating_sub(bytes);
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn reserved_peak(&self) -> usize {
        self.inner.reserved_peak()
    }

    fn memory_profile_snapshot(&self) -> Option<MemoryProfileSnapshot> {
        let mut top_consumers: Vec<(String, u64)> = self
            .consumers
            .lock()
            .expect("memory pool mutex poisoned")
            .iter()
            .map(|(name, profile)| (name.clone(), profile.peak))
            .collect();
        top_consumers.extend(
            live_bytes_snapshot()
                .into_iter()
                .filter(|(_, bytes)| *bytes > 0)
                .map(|(label, bytes)| (label.to_string(), bytes)),
        );
        top_consumers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        top_consumers.truncate(5);

        Some(MemoryProfileSnapshot {
            pool_peak_bytes: self.peak_reserved.load(Ordering::Relaxed),
            jemalloc_resident_peak_bytes: self.jemalloc_resident_peak.load(Ordering::Relaxed),
            jemalloc_allocated_peak_bytes: self.jemalloc_allocated_peak.load(Ordering::Relaxed),
            jemalloc_active_peak_bytes: self.jemalloc_active_peak.load(Ordering::Relaxed),
            jemalloc_retained_peak_bytes: self.jemalloc_retained_peak.load(Ordering::Relaxed),
            top_consumers,
        })
    }
}

impl fmt::Display for MemoryProfilePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryProfilePool({})", self.inner.reserved())
    }
}

#[derive(Debug, Copy, Clone)]
struct JemallocMemoryStats {
    resident: u64,
    allocated: u64,
    active: u64,
    retained: u64,
}

#[cfg(not(target_env = "msvc"))]
fn jemalloc_memory_stats() -> Option<JemallocMemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    #[derive(Copy, Clone)]
    struct JemallocMemoryMibs {
        advance: tikv_jemalloc_ctl::epoch_mib,
        resident: tikv_jemalloc_ctl::stats::resident_mib,
        allocated: tikv_jemalloc_ctl::stats::allocated_mib,
        active: tikv_jemalloc_ctl::stats::active_mib,
        retained: tikv_jemalloc_ctl::stats::retained_mib,
    }

    static MIBS: OnceLock<Option<JemallocMemoryMibs>> = OnceLock::new();
    let mibs = (*MIBS.get_or_init(|| {
        Some(JemallocMemoryMibs {
            advance: epoch::mib().ok()?,
            resident: stats::resident::mib().ok()?,
            allocated: stats::allocated::mib().ok()?,
            active: stats::active::mib().ok()?,
            retained: stats::retained::mib().ok()?,
        })
    }))?;

    if mibs.advance.advance().is_err() {
        return None;
    }

    Some(JemallocMemoryStats {
        resident: mibs.resident.read().ok()? as u64,
        allocated: mibs.allocated.read().ok()? as u64,
        active: mibs.active.read().ok()? as u64,
        retained: mibs.retained.read().ok()? as u64,
    })
}

#[cfg(target_env = "msvc")]
fn jemalloc_memory_stats() -> Option<JemallocMemoryStats> {
    None
}
