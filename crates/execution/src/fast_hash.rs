//! Internal aliases for non-cryptographic fast hashing.
//!
//! Arneb accepts only authoritative internal SQL — there is no public
//! endpoint where adversarial input could trigger hash-flooding. The
//! standard library's `DefaultHasher` (SipHash) is therefore pure
//! overhead on every hash-table operation in `crates/execution/`.
//!
//! These aliases swap SipHash for AHash on every hot-path hash table:
//! `JoinHashMap`, `HashAggregateExec` group map, `DistinctAccumulator`
//! seen-set, semi-join membership set, set-operation dedup sets, and
//! window partition hashing. AHash is 3–5× faster than SipHash on
//! x86_64 (AES-NI) and ARM64 (NEON) with no transitive dependency cost.
//!
//! API shape mirrors `std::collections::HashMap` / `HashSet`, so call
//! sites only swap the type, not the methods. `RandomState` is seeded
//! per-process; hash values are never persisted, so randomization is
//! safe.
//!
//! Tracked under `openspec/changes/exec-ahash-hashtables/`.

#![allow(dead_code)] // alias module — items used across submodules.

/// AHash variant of `std::hash::Hasher`, for ad-hoc per-row hashing.
pub(crate) type FastHasher = ahash::AHasher;

/// AHash-backed replacement for `std::collections::HashMap<K, V>`.
pub(crate) type FastHashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

/// AHash-backed replacement for `std::collections::HashSet<K>`.
pub(crate) type FastHashSet<K> = std::collections::HashSet<K, ahash::RandomState>;
