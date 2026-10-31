#!/usr/bin/env bash
# Agent query API smoke test (/api/v2 catalogs, explain, statements).
#
# Runs the acceptance scenario of the agent-query-api change against a live
# coordinator (or standalone) serving the hive TPC-H tables. Uses curl and
# jq for the API, and psql once to cross-check Q6 over pgwire. Prints
# `PASS <step>` per check and exits non-zero at the first `FAIL <step>`.
#
# Environment:
#   ARNEB_URL        Web port base URL, e.g. http://127.0.0.1:6432 (required)
#   ARNEB_TOKEN      API token of the first user, or
#   ARNEB_USER / ARNEB_PASSWORD   that user's password (HTTP Basic)
#   ARNEB_TOKEN2     credentials of a second user (ownership check), or
#   ARNEB_USER2 / ARNEB_PASSWORD2
#   ARNEB_PG         libpq conninfo for psql, e.g.
#                    "host=127.0.0.1 port=5432 user=alice dbname=x" (required);
#                    the password is ARNEB_PASSWORD, else ARNEB_TOKEN
#   ARNEB_CATALOG    TPC-H catalog (default datalake)
#   ARNEB_SCHEMA     TPC-H schema (default tpch)
#   ARNEB_LONG_SQL   the long statement to cancel (default: 3-way self-join
#                    of lineitem)
#   ARNEB_SMOKE_TIMEOUT_SECS  per-statement timeout (default 900)

set -euo pipefail

: "${ARNEB_URL:?set ARNEB_URL to the Web port, e.g. http://127.0.0.1:6432}"
: "${ARNEB_PG:?set ARNEB_PG to a libpq conninfo for the pgwire port}"
CATALOG="${ARNEB_CATALOG:-datalake}"
SCHEMA="${ARNEB_SCHEMA:-tpch}"
TIMEOUT="${ARNEB_SMOKE_TIMEOUT_SECS:-900}"
LONG_SQL="${ARNEB_LONG_SQL:-SELECT count(*) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey JOIN lineitem c ON b.l_orderkey = c.l_orderkey}"
API="${ARNEB_URL%/}/api/v2"
TPCH="$API/catalogs/$CATALOG/schemas/$SCHEMA"
Q6="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24"

auth_args() { # $1 = token, $2 = user, $3 = password
    if [ -n "$1" ]; then
        printf '%s\n' -H "Authorization: Bearer $1"
    elif [ -n "$2" ]; then
        printf '%s\n' -u "$2:$3"
    else
        echo "missing credentials" >&2
        exit 2
    fi
}
AUTH1=()
while IFS= read -r a; do AUTH1+=("$a"); done < <(auth_args "${ARNEB_TOKEN:-}" "${ARNEB_USER:-}" "${ARNEB_PASSWORD:-}")
AUTH2=()
while IFS= read -r a; do AUTH2+=("$a"); done < <(auth_args "${ARNEB_TOKEN2:-}" "${ARNEB_USER2:-}" "${ARNEB_PASSWORD2:-}")

STEP=""
LAST=""
pass() { echo "PASS $STEP"; }
fail() {
    echo "FAIL $STEP: $*"
    [ -n "$LAST" ] && echo "  last response: $(printf '%s' "$LAST" | head -c 2000)"
    exit 1
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}

# req <AUTH1|AUTH2> <method> <url> [json-body]: body to $LAST, status to $CODE.
req() {
    local out auth=("${AUTH1[@]}")
    [ "$1" = AUTH2 ] && auth=("${AUTH2[@]}")
    if [ $# -ge 4 ]; then
        out=$(curl -sS -w '\n%{http_code}' "${auth[@]}" -X "$2" "$3" \
            -H 'Content-Type: application/json' -d "$4") || fail "curl $2 $3"
    else
        out=$(curl -sS -w '\n%{http_code}' "${auth[@]}" -X "$2" "$3") || fail "curl $2 $3"
    fi
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
}

# submit <sql> [max_rows]: POST /statements, sets $ID from the status.
submit() {
    local body
    body=$(jq -nc --arg sql "$1" --arg c "$CATALOG" --arg s "$SCHEMA" --arg m "${2:-}" \
        '{sql:$sql, catalog:$c, schema:$s} + (if $m == "" then {} else {max_rows:($m|tonumber)} end)')
    req AUTH1 POST "$API/statements?wait_ms=2000" "$body"
    [ "$CODE" = 201 ] || fail "submit returned HTTP $CODE"
    ID=$(printf '%s' "$LAST" | jq -r '.id')
}

# wait_terminal: long-poll $ID until finished, failed or cancelled.
wait_terminal() {
    local deadline=$(( $(date +%s) + TIMEOUT )) state
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        case "$state" in finished|failed|cancelled) return 0 ;; esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $ID still $state after ${TIMEOUT}s"
        req AUTH1 GET "$API/statements/$ID?wait_ms=5000"
        [ "$CODE" = 200 ] || fail "poll returned HTTP $CODE"
    done
}

# first_cell: the single value of a finished one-row statement, as text.
first_cell() {
    local rows
    rows=$(printf '%s' "$LAST" | jq -c '.first_page.rows // empty')
    if [ -z "$rows" ]; then
        req AUTH1 GET "$API/statements/$ID/results?cursor=0"
        [ "$CODE" = 200 ] || fail "results returned HTTP $CODE"
        rows=$(printf '%s' "$LAST" | jq -c '.rows')
    fi
    printf '%s' "$rows" | jq -r '.[0][0] | tostring'
}

STEP="1 catalog discovery"
req AUTH1 GET "$API/catalogs"
[ "$CODE" = 200 ] || fail "GET /catalogs returned HTTP $CODE"
check "any(.items[]; .name == \"$CATALOG\")" "catalog $CATALOG not listed"
req AUTH1 GET "$TPCH/tables?prefix=line"
[ "$CODE" = 200 ] || fail "GET tables returned HTTP $CODE"
check 'any(.items[]; .name == "lineitem")' "lineitem not listed"
req AUTH1 GET "$TPCH/tables/lineitem"
[ "$CODE" = 200 ] || fail "describe lineitem returned HTTP $CODE"
check 'any(.columns[]; .name == "l_quantity")' "l_quantity not described"
pass

STEP="2 sample"
req AUTH1 GET "$TPCH/tables/nation/sample?rows=5"
[ "$CODE" = 200 ] || fail "sample returned HTTP $CODE"
ID=$(printf '%s' "$LAST" | jq -r '.id')
wait_terminal
check '.state == "finished"' "sample did not finish"
if ! printf '%s' "$LAST" | jq -e '.first_page' >/dev/null; then
    req AUTH1 GET "$API/statements/$ID/results"
    check '.rows | length == 5' "sample did not return 5 rows"
else
    check '.first_page.rows | length == 5' "sample did not return 5 rows"
fi
pass

STEP="3 explain"
req AUTH1 POST "$API/explain" "$(jq -nc --arg sql "$Q6" --arg c "$CATALOG" --arg s "$SCHEMA" '{sql:$sql, catalog:$c, schema:$s}')"
[ "$CODE" = 200 ] || fail "explain returned HTTP $CODE"
check '.plan | length > 0' "empty plan"
check "any(.scans[]; .table == \"$CATALOG.$SCHEMA.lineitem\")" "lineitem scan not listed"
pass

STEP="4 submit Q6"
submit "$Q6"
wait_terminal
check '.state == "finished"' "Q6 did not finish"
API_REVENUE=$(first_cell)
PG_REVENUE=$(PGPASSWORD="${ARNEB_PASSWORD:-${ARNEB_TOKEN:-}}" psql "$ARNEB_PG" -XtAq \
    -c "${Q6/FROM lineitem/FROM $CATALOG.$SCHEMA.lineitem}") || fail "psql Q6 failed"
[ "$API_REVENUE" = "$PG_REVENUE" ] || fail "revenue differs: api=$API_REVENUE psql=$PG_REVENUE"
echo "  revenue api=$API_REVENUE psql=$PG_REVENUE"
pass

STEP="5 truncation"
submit "SELECT * FROM nation" 100
wait_terminal
check '.state == "finished"' "SELECT * FROM nation did not finish"
check '.truncated == false and .total_rows == 25' "nation is not 25 untruncated rows"
submit "SELECT * FROM lineitem" 100
wait_terminal
check '.state == "finished"' "SELECT * did not finish"
check '.truncated == true' "not truncated"
check '.total_rows == null' "total_rows is not null for a truncated result"
ELAPSED=$(printf '%s' "$LAST" | jq -r '.elapsed_ms')
ROWS=0
CURSOR=0
PAGES=0
while [ -n "$CURSOR" ] && [ "$CURSOR" != null ]; do
    req AUTH1 GET "$API/statements/$ID/results?cursor=$CURSOR&max_bytes=8192"
    [ "$CODE" = 200 ] || fail "results cursor=$CURSOR returned HTTP $CODE"
    ROWS=$(( ROWS + $(printf '%s' "$LAST" | jq '.rows | length') ))
    CURSOR=$(printf '%s' "$LAST" | jq -r '.next_cursor')
    PAGES=$(( PAGES + 1 ))
    [ "$PAGES" -le 200 ] || fail "too many pages"
done
LAST=""
[ "$ROWS" = 100 ] || fail "fetched $ROWS rows, want 100"
echo "  elapsed_ms=$ELAPSED rows=$ROWS pages=$PAGES"
pass

STEP="6 bad column"
BAD_SQL="SELECT l_quantiy FROM lineitem"
body=$(jq -nc --arg sql "$BAD_SQL" --arg c "$CATALOG" --arg s "$SCHEMA" '{sql:$sql, catalog:$c, schema:$s}')
req AUTH1 POST "$API/statements?wait_ms=2000" "$body"
if [ "$CODE" = 201 ]; then
    ID=$(printf '%s' "$LAST" | jq -r '.id')
    wait_terminal
    check '.state == "failed"' "bad column did not fail"
fi
check '.error.code == "column_not_found"' "code is not column_not_found"
check '.error.location != null' "no location"
check '.error.candidates[0] == "l_quantity"' "candidates[0] is not l_quantity"
CANDIDATE=$(printf '%s' "$LAST" | jq -r '.error.candidates[0]')
echo "  error: $(printf '%s' "$LAST" | jq -c '.error | {code, location, candidates}')"
pass

STEP="7 fix from hint"
submit "${BAD_SQL/l_quantiy/$CANDIDATE} LIMIT 10"
wait_terminal
check '.state == "finished"' "fixed statement did not finish"
OWNED_ID="$ID"
pass

STEP="8 long statement cancel"
submit "$LONG_SQL"
deadline=$(( $(date +%s) + 60 ))
while :; do
    state=$(printf '%s' "$LAST" | jq -r '.state')
    [ "$state" = running ] && break
    [ "$state" = queued ] || fail "long statement is $state, not running"
    [ "$(date +%s)" -lt "$deadline" ] || fail "long statement never running"
    req AUTH1 GET "$API/statements/$ID?wait_ms=200"
done
echo "  observed running: id=$ID elapsed_ms=$(printf '%s' "$LAST" | jq '.elapsed_ms')"
req AUTH1 DELETE "$API/statements/$ID"
[ "$CODE" = 200 ] || fail "cancel returned HTTP $CODE"
check '.state == "cancelled"' "cancel did not answer cancelled"
req AUTH1 GET "$API/statements/$ID"
check '.state == "cancelled"' "status is not cancelled"
req AUTH1 GET "$API/queries/$ID"
[ "$CODE" = 200 ] || fail "GET /queries/$ID returned HTTP $CODE"
check '(.state | ascii_upcase) == "CANCELLED"' "/api/v2/queries does not show cancelled"
pass

STEP="9 ownership"
req AUTH2 GET "$API/statements/$OWNED_ID"
[ "$CODE" = 404 ] || fail "second user got HTTP $CODE, want 404"
check '.error.code == "not_found"' "code is not not_found"
pass
