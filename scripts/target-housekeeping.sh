#!/usr/bin/env bash
# target-housekeeping.sh — report Rust build-artifact size and flag when
# a `cargo clean` is overdue.
#
# WHY: cargo never garbage-collects old artifacts. `target/debug/deps`
# accumulates a new hash-versioned .rlib + test binary on every build,
# and `target/debug/incremental` is an unbounded cache. Over hundreds of
# builds this grows to 100s of GB. Cleaning is safe: it only forces one
# full recompile. The docker bench has its OWN target inside the image,
# so cleaning the local target/ never affects a running benchmark.
#
# Usage:
#   ./scripts/target-housekeeping.sh          # report only
#   ./scripts/target-housekeeping.sh --sweep  # report, then GC stale
#                                             #   artifacts (cargo sweep
#                                             #   --time 14: keep last 14d)
#   ./scripts/target-housekeeping.sh --clean  # report, then `cargo clean`
#                                             #   (nukes everything)
#
# Prefer --sweep for routine GC: it removes artifacts not touched in 14
# days but keeps recent ones, so the next build stays fast. Use --clean
# only when target/ is egregiously large or you want a clean slate.
#
# Suggested cadence: run the report at the start of a work session, or
# whenever disk feels tight. --sweep when it crosses 🟡; --clean at 🔴.
# (incremental compilation is disabled via .cargo/config.toml, which
# removes the other unbounded half — target/debug/incremental.)

set -u
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

WARN_GB=${WARN_GB:-40}     # yellow: worth cleaning soon
URGE_GB=${URGE_GB:-80}     # red: clean now

human() { du -sh "$1" 2>/dev/null | cut -f1; }
gb_of() { du -sk "$1" 2>/dev/null | awk '{printf "%.0f", $1/1024/1024}'; }

[[ -d target ]] || { echo "No target/ dir — nothing to clean."; exit 0; }

total_gb=$(gb_of target)
echo "Rust build artifacts in $PROJECT_DIR/target"
echo "  target/debug          $(human target/debug)"
echo "    ├─ deps             $(human target/debug/deps)    ($(ls target/debug/deps 2>/dev/null | wc -l | tr -d ' ') files)"
echo "    └─ incremental      $(human target/debug/incremental)"
echo "  target/release        $(human target/release)"
echo "  ----------------------------------------"
echo "  TOTAL                 ${total_gb} GB"
echo

if   (( total_gb >= URGE_GB )); then
    echo "🔴 OVERDUE (>= ${URGE_GB} GB). Run: cargo clean   (reclaims ~${total_gb} GB; one full rebuild next time)"
    verdict=red
elif (( total_gb >= WARN_GB )); then
    echo "🟡 Worth cleaning soon (>= ${WARN_GB} GB). Run: cargo clean   when convenient."
    verdict=yellow
else
    echo "🟢 Healthy (< ${WARN_GB} GB). No action needed."
    verdict=green
fi

case "${1:-}" in
  --sweep)
    echo
    if command -v cargo-sweep >/dev/null 2>&1 || cargo sweep --version >/dev/null 2>&1; then
        echo "Running cargo sweep --time 14 (GC artifacts unused > 14 days) ..."
        cargo sweep --time 14
        echo "Swept. Now: $(human target)"
    else
        echo "cargo-sweep not installed. Install once: cargo install cargo-sweep"
        echo "Or fall back to a full clean: $0 --clean"
    fi
    ;;
  --clean)
    echo
    echo "Running cargo clean (full wipe) ..."
    if command -v cargo >/dev/null 2>&1; then
        cargo clean && echo "Done. Reclaimed ~${total_gb} GB."
    else
        # Pre-commit-hook environments sometimes lack cargo on PATH.
        echo "cargo not on PATH — falling back to: rm -rf target/debug/incremental target/debug/deps"
        rm -rf target/debug/incremental target/debug/deps && echo "Removed deps + incremental."
    fi
    ;;
esac

# Machine-readable exit: 0 green, 10 yellow, 20 red (for cron/CI hooks).
case "${verdict:-green}" in green) exit 0;; yellow) exit 10;; red) exit 20;; esac
