#!/usr/bin/env bash
#
# End-to-end integration tests for trino-alt.
# Assumes the server is already running with test data loaded.
#
# Prerequisites:
#   - psql (brew install libpq)
#   - trino-alt running with orders + items tables
#
# Usage:
#   # Terminal 1: start server
#   cargo run --release -- --config test-config.toml
#
#   # Terminal 2: run tests
#   ./scripts/integration-test.sh [host] [port] [table_prefix]
#
# Defaults: host=127.0.0.1, port=5432, table_prefix=file.default

set -euo pipefail

HOST="${1:-127.0.0.1}"
PORT="${2:-5432}"
PREFIX="${3:-file.default}"

T_ORDERS="$PREFIX.orders"
T_ITEMS="$PREFIX.items"

PASS=0
FAIL=0

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
run_sql() {
    psql -h "$HOST" -p "$PORT" -U test -d test -t -A -c "$1" 2>&1
}

assert_row_count() {
    local test_name="$1"
    local expected_count="$2"
    local sql="$3"

    local result
    result=$(run_sql "$sql") || true
    local actual_count
    actual_count=$(echo "$result" | grep -c '[^ ]' || true)

    if [ "$actual_count" -eq "$expected_count" ]; then
        echo -e "  ${GREEN}PASS${NC} $test_name (${actual_count} rows)"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $test_name — expected ${expected_count} rows, got ${actual_count}"
        echo -e "       sql: $sql"
        echo -e "       output: $result"
        FAIL=$((FAIL + 1))
    fi
}

assert_eq() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"

    if [ "$actual" = "$expected" ]; then
        echo -e "  ${GREEN}PASS${NC} $test_name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $test_name — expected: '$expected', got: '$actual'"
        FAIL=$((FAIL + 1))
    fi
}

assert_contains() {
    local test_name="$1"
    local expected_substr="$2"
    local actual="$3"

    if echo "$actual" | grep -q "$expected_substr"; then
        echo -e "  ${GREEN}PASS${NC} $test_name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC} $test_name — expected to contain '$expected_substr'"
        echo -e "       actual: $actual"
        FAIL=$((FAIL + 1))
    fi
}

# ---------------------------------------------------------------------------
# Connectivity check
# ---------------------------------------------------------------------------
echo -e "${BOLD}Connecting to $HOST:$PORT ...${NC}"
if ! psql -h "$HOST" -p "$PORT" -U test -d test -c "" >/dev/null 2>&1; then
    echo -e "${RED}ERROR: Cannot connect to trino-alt at $HOST:$PORT${NC}"
    echo "Start the server first, e.g.:"
    echo "  cargo run --release -- --config trino-alt.toml"
    exit 1
fi
echo ""

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

echo -e "${BOLD}--- Basic Queries ---${NC}"
assert_row_count "SELECT *" 5 "SELECT * FROM $T_ORDERS"
assert_row_count "WHERE filter" 2 "SELECT * FROM $T_ORDERS WHERE id > 3"
assert_row_count "Projection (2 cols)" 5 "SELECT customer, total FROM $T_ORDERS"
assert_row_count "Single column" 5 "SELECT customer FROM $T_ORDERS"

echo ""
echo -e "${BOLD}--- Sorting & Limits ---${NC}"
assert_row_count "ORDER BY" 5 "SELECT * FROM $T_ORDERS ORDER BY total DESC"
assert_row_count "LIMIT" 3 "SELECT * FROM $T_ORDERS LIMIT 3"
assert_row_count "LIMIT + OFFSET" 2 "SELECT * FROM $T_ORDERS ORDER BY id LIMIT 2 OFFSET 1"

# Verify ORDER BY correctness: first row should be dave (id=4, total=300).
FIRST=$(run_sql "SELECT * FROM $T_ORDERS ORDER BY total DESC LIMIT 1" | head -1 | cut -d'|' -f2 | tr -d '[:space:]')
assert_eq "ORDER BY DESC correctness" "dave" "$FIRST"

echo ""
echo -e "${BOLD}--- Joins ---${NC}"
assert_row_count "CROSS JOIN" 30 "SELECT * FROM $T_ORDERS a, $T_ITEMS b"
assert_row_count "INNER JOIN (equi)" 6 \
    "SELECT o.customer, i.product FROM $T_ORDERS o JOIN $T_ITEMS i ON o.id = i.order_id"
assert_row_count "JOIN + WHERE" 2 \
    "SELECT o.customer, i.product FROM $T_ORDERS o JOIN $T_ITEMS i ON o.id = i.order_id WHERE o.id = 1"

echo ""
echo -e "${BOLD}--- EXPLAIN ---${NC}"
EXPLAIN=$(run_sql "EXPLAIN SELECT * FROM $T_ORDERS WHERE id > 2")
assert_contains "EXPLAIN shows TableScan" "TableScan" "$EXPLAIN"
assert_contains "EXPLAIN shows Filter" "Filter" "$EXPLAIN"

echo ""
echo -e "${BOLD}--- Error Handling ---${NC}"
ERR1=$(run_sql "SELECT * FROM nonexistent_table" 2>&1 || true)
assert_contains "Unknown table → ERROR" "ERROR" "$ERR1"

ERR2=$(run_sql "SELEC bad syntax" 2>&1 || true)
assert_contains "Syntax error → ERROR" "ERROR" "$ERR2"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
TOTAL=$((PASS + FAIL))
echo -e "${BOLD}=====================================${NC}"
echo -e " ${GREEN}PASSED: $PASS${NC} / $TOTAL"
if [ "$FAIL" -gt 0 ]; then
    echo -e " ${RED}FAILED: $FAIL${NC}"
fi
echo -e "${BOLD}=====================================${NC}"

[ "$FAIL" -eq 0 ]
