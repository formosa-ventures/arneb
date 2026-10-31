#!/usr/bin/env bash
# Manage API smoke test (/api/v2 workers, admission, audit, cancel; MCP).
#
# Runs the acceptance scenario of the manage-api change against a live
# distributed stack: a coordinator plus two workers (ids worker-1 and
# worker-2) with password auth, the hive TPC-H tables, a rules file whose
# `tables` section grants user2 SELECT only, `admin_users = [<admin>]` and a
# persistent `[state] path`. Uses curl and jq for the API and psql for the
# pgwire checks. Prints `PASS <step>` per check and exits non-zero at the
# first `FAIL <step>`. Passwords are never printed.
#
# Environment:
#   ARNEB_URL             Web port base URL, e.g. http://127.0.0.1:6432 (required)
#   ARNEB_PG              host:port of the pgwire port, e.g. 127.0.0.1:5442 (required)
#   ARNEB_ADMIN_USER / ARNEB_ADMIN_PASSWORD   a user in admin_users (required)
#   ARNEB_USER2 / ARNEB_PASSWORD2             a user without manage, SELECT only (required)
#   ARNEB_CATALOG         TPC-H catalog (default datalake)
#   ARNEB_SCHEMA          TPC-H schema (default tpch)
#   ARNEB_LONG_SQL        the long statement (default: 3-way self-join of lineitem)
#   ARNEB_WORKER1 / ARNEB_WORKER2   worker ids (default worker-1 / worker-2)
#   ARNEB_SMOKE_TIMEOUT_SECS  per-statement timeout (default 900)
#   KEEP_CORDON=1         leave worker-1 cordoned at the end of the full run,
#                         for the restart check
#
# Usage:
#   scripts/manage-smoke.sh                 steps 1 to 9
#   KEEP_CORDON=1 scripts/manage-smoke.sh   the same, worker-1 left cordoned
#   scripts/manage-smoke.sh --after-restart step 10 only, after restarting the
#                                           coordinator (not the workers)

set -euo pipefail

AFTER_RESTART=0
case "${1:-}" in
    "") ;;
    --after-restart) AFTER_RESTART=1 ;;
    *) echo "usage: $0 [--after-restart]" >&2; exit 2 ;;
esac

: "${ARNEB_URL:?set ARNEB_URL to the Web port, e.g. http://127.0.0.1:6432}"
: "${ARNEB_PG:?set ARNEB_PG to host:port of the pgwire port}"
: "${ARNEB_ADMIN_USER:?set ARNEB_ADMIN_USER to a user in admin_users}"
: "${ARNEB_ADMIN_PASSWORD:?set ARNEB_ADMIN_PASSWORD}"
: "${ARNEB_USER2:?set ARNEB_USER2 to a user without manage}"
: "${ARNEB_PASSWORD2:?set ARNEB_PASSWORD2}"
CATALOG="${ARNEB_CATALOG:-datalake}"
SCHEMA="${ARNEB_SCHEMA:-tpch}"
TIMEOUT="${ARNEB_SMOKE_TIMEOUT_SECS:-900}"
W1="${ARNEB_WORKER1:-worker-1}"
W2="${ARNEB_WORKER2:-worker-2}"
LONG_SQL="${ARNEB_LONG_SQL:-SELECT count(*) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey JOIN lineitem c ON b.l_orderkey = c.l_orderkey}"
API="${ARNEB_URL%/}/api/v2"
MCP="${ARNEB_URL%/}/mcp"
ADMIN="$ARNEB_ADMIN_USER"
USER2="$ARNEB_USER2"
PG_HOST="${ARNEB_PG%:*}"
PG_PORT="${ARNEB_PG##*:}"
Q6="SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24"

STEP=""
LAST=""
CODE=""
ID=""
RID=0
pass() { echo "PASS $STEP"; }
fail() {
    echo "FAIL $STEP: $*"
    [ -n "$LAST" ] && echo "  last response (HTTP ${CODE:-?}): $(printf '%s' "$LAST" | head -c 2000)"
    exit 1
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}

# req <ADMIN|USER2> <method> <url> [json-body]: body to $LAST, status to $CODE.
req() {
    local out user pw
    case "$1" in
        ADMIN) user="$ADMIN"; pw="$ARNEB_ADMIN_PASSWORD" ;;
        *) user="$USER2"; pw="$ARNEB_PASSWORD2" ;;
    esac
    if [ $# -ge 4 ]; then
        out=$(curl -sS -w '\n%{http_code}' -u "$user:$pw" -X "$2" "$3" \
            -H 'Content-Type: application/json' -d "$4") || fail "curl $2 $3"
    else
        out=$(curl -sS -w '\n%{http_code}' -u "$user:$pw" -X "$2" "$3") || fail "curl $2 $3"
    fi
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
}

# expect_code <code> <what>: the last request must have answered this status.
expect_code() { [ "$CODE" = "$1" ] || fail "$2 returned HTTP $CODE, want $1"; }

# submit <sql>: POST /statements as user2, sets $ID; $LAST is the status.
submit() {
    local body
    body=$(jq -nc --arg sql "$1" --arg c "$CATALOG" --arg s "$SCHEMA" \
        '{sql:$sql, catalog:$c, schema:$s}')
    req USER2 POST "$API/statements?wait_ms=${2:-2000}" "$body"
    case "$CODE" in 200 | 201) ;; *) fail "submit returned HTTP $CODE" ;; esac
    ID=$(printf '%s' "$LAST" | jq -r '.id')
}

# poll: refresh $LAST with the status of statement $ID (user2).
poll() {
    req USER2 GET "$API/statements/$ID?wait_ms=${1:-500}"
    expect_code 200 "GET /statements/$ID"
}

# wait_state <state> <seconds>: poll $ID until it reports <state>; fail if it
# ends in another terminal state or the time runs out.
wait_state() {
    local deadline=$(( $(date +%s) + $2 )) state
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        [ "$state" = "$1" ] && return 0
        case "$state" in
            finished | failed | cancelled) fail "statement $ID ended $state while waiting for $1" ;;
        esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $ID is $state after $2 s, want $1"
        poll 300
    done
}

# wait_terminal: poll $ID until finished, failed or cancelled.
wait_terminal() {
    local deadline=$(( $(date +%s) + TIMEOUT )) state
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        case "$state" in finished | failed | cancelled) return 0 ;; esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $ID still $state after ${TIMEOUT} s"
        poll 5000
    done
}

# wait_worker <id> <jq-filter> <seconds> <what>: poll GET /workers/<id> as the
# admin until the filter holds.
wait_worker() {
    local deadline=$(( $(date +%s) + $3 ))
    while :; do
        req ADMIN GET "$API/workers/$1"
        if [ "$CODE" = 200 ] && printf '%s' "$LAST" | jq -e "$2" >/dev/null 2>&1; then
            return 0
        fi
        [ "$(date +%s)" -lt "$deadline" ] || fail "worker $1: $4 not reached after $3 s"
        sleep 0.5
    done
}

# manage <POST|PUT|DELETE> <path> [json-body]: a real manage call as the admin
# that must answer 200 with a non-dry envelope.
manage() {
    req ADMIN "$@"
    expect_code 200 "$1 $2"
    check '.dry_run == false' "$1 $2 answered a dry run"
}

# first_cell: the single value of a finished one-row statement, as text.
first_cell() {
    local rows
    rows=$(printf '%s' "$LAST" | jq -c '.first_page.rows // empty')
    if [ -z "$rows" ]; then
        req USER2 GET "$API/statements/$ID/results?cursor=0"
        expect_code 200 "GET /statements/$ID/results"
        rows=$(printf '%s' "$LAST" | jq -c '.rows')
    fi
    printf '%s' "$rows" | jq -r '.[0][0] | tostring'
}

# user2 psql: psql_user2 <sql>; output to $PSQL_OUT, exit status to $PSQL_RC.
PSQL_OUT=""
PSQL_RC=0
psql_user2() {
    PSQL_RC=0
    PSQL_OUT=$(PGPASSWORD="$ARNEB_PASSWORD2" PGCONNECT_TIMEOUT=10 psql -X -tAq \
        -v VERBOSITY=sqlstate -h "$PG_HOST" -p "$PG_PORT" -U "$USER2" -d arneb \
        -c "$1" 2>&1) || PSQL_RC=$?
}

# MCP: mcp_tool <ADMIN|USER2> <name> [args-json]: tools/call, $LAST = result.
mcp_tool() {
    local user pw out
    case "$1" in
        ADMIN) user="$ADMIN"; pw="$ARNEB_ADMIN_PASSWORD" ;;
        *) user="$USER2"; pw="$ARNEB_PASSWORD2" ;;
    esac
    local targs="${3:-}"
    [ -n "$targs" ] || targs='{}'
    RID=$(( RID + 1 ))
    out=$(curl -sS -w '\n%{http_code}' -u "$user:$pw" -X POST "$MCP" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json, text/event-stream' \
        -H 'MCP-Protocol-Version: 2025-06-18' \
        -d "$(jq -nc --argjson id "$RID" --arg n "$2" --argjson a "$targs" \
            '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:$n, arguments:$a}}')") \
        || fail "curl POST /mcp"
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
    expect_code 200 "tools/call $2"
    check '.error == null' "tools/call $2 returned a JSON-RPC error"
    LAST=$(printf '%s' "$LAST" | jq -c '.result')
}

step10() {
    STEP="10 state survives a coordinator restart"
    # The worker may still be re-registering: the stored cordon applies at once.
    wait_worker "$W1" '.scheduling == "cordoned"' 60 "scheduling cordoned (the stored cordon was lost)"
    wait_worker "$W1" '.registered == true' 90 "re-registration"
    check '.scheduling == "cordoned"' "$W1 is no longer cordoned after it re-registered"
    req ADMIN GET "$API/audit?limit=50"
    expect_code 200 "GET /audit"
    for a in worker.cordon worker.drain worker.uncordon query.cancel admission.set; do
        check "any(.entries[]; .action == \"$a\" and .user == \"$ADMIN\")" \
            "audit entry $a by $ADMIN did not survive the restart"
    done
    check '.persistent == true' "the audit log is not persistent (no [state] path?)"
    req ADMIN GET "$API/admission"
    expect_code 200 "GET /admission"
    check '.source == "state"' "admission source is not state after the restart"
    # Clean up.
    manage POST "$API/workers/$W1/uncordon"
    check '.result.scheduling == "schedulable"' "uncordon of $W1 did not make it schedulable"
    pass
}

if [ "$AFTER_RESTART" = 1 ]; then
    step10
    exit 0
fi

STEP="1 whoami"
req ADMIN GET "$API/whoami"
expect_code 200 "admin whoami"
check '.user == "'"$ADMIN"'"' "admin whoami names another user"
check '.privileges.manage == true' "admin privileges.manage is not true (is $ADMIN in admin_users?)"
req USER2 GET "$API/whoami"
expect_code 200 "user2 whoami"
check '.privileges.manage == false' "user2 privileges.manage is not false"
pass

STEP="2 non-manager is refused"
req USER2 POST "$API/workers/$W1/cordon"
expect_code 403 "user2 cordon"
check '.error.code == "forbidden"' "user2 cordon error code is not forbidden"
req USER2 POST "$API/workers/$W1/cordon?dry_run=true"
expect_code 403 "user2 dry-run cordon"
check '.error.code == "forbidden"' "user2 dry-run cordon error code is not forbidden"
req ADMIN GET "$API/workers/$W1"
expect_code 200 "GET /workers/$W1"
check '.scheduling == "schedulable"' "$W1 is not schedulable at the start (leftover cordon from an earlier run?)"
pass

STEP="3 dry run changes nothing"
req ADMIN POST "$API/workers/$W1/cordon?dry_run=true"
expect_code 200 "admin dry-run cordon"
check '.dry_run == true and .action == "worker.cordon"' "dry-run envelope is wrong"
check '(has("result") | not) or .result == null' "a dry run returned a result"
req ADMIN GET "$API/workers/$W1"
check '.scheduling == "schedulable"' "$W1 is no longer schedulable after a dry run"
pass

STEP="4 cordon steers new queries away"
manage POST "$API/workers/$W1/cordon"
check '.action == "worker.cordon" and .changed == true' "cordon envelope is wrong"
check '.result.scheduling == "cordoned"' "cordon result is not cordoned"
submit "$LONG_SQL"
wait_state running 60
LONG_ID="$ID"
echo "  long statement running: id=$LONG_ID"
wait_worker "$W2" '.running_queries >= 1' 20 "running_queries >= 1"
req ADMIN GET "$API/workers/$W1"
check '.running_queries == 0' "$W1 runs $(printf '%s' "$LAST" | jq -r '.running_queries') queries while cordoned, want 0"
pass

STEP="5 drain, kill and uncordon"
manage POST "$API/workers/$W2/drain"
check '.action == "worker.drain"' "drain envelope is wrong"
check '.result.scheduling == "draining"' "drain result is not draining (running query on $W2?)"
manage DELETE "$API/queries/$LONG_ID"
check '.action == "query.cancel" and .changed == true' "kill did not report changed: true"
ID="$LONG_ID"
poll 1000
wait_state cancelled 30
wait_worker "$W2" '.scheduling == "drained"' 30 "scheduling drained"
manage POST "$API/workers/$W1/uncordon"
check '.result.scheduling == "schedulable"' "uncordon of $W1 did not make it schedulable"
manage POST "$API/workers/$W2/uncordon"
check '.result.scheduling == "schedulable"' "uncordon of $W2 did not make it schedulable"
submit "$Q6"
wait_terminal
check '.state == "finished"' "Q6 did not finish after the uncordon"
API_REVENUE=$(first_cell)
psql_user2 "${Q6/FROM lineitem/FROM $CATALOG.$SCHEMA.lineitem}"
[ "$PSQL_RC" = 0 ] || fail "psql Q6 failed: $PSQL_OUT"
[ "$API_REVENUE" = "$PSQL_OUT" ] || fail "Q6 revenue differs: api=$API_REVENUE psql=$PSQL_OUT"
echo "  revenue=$API_REVENUE"
pass

STEP="6 admission limits"
req ADMIN GET "$API/admission"
expect_code 200 "GET /admission"
ORIG_RUN=$(printf '%s' "$LAST" | jq -r '.max_running_queries')
ORIG_QUEUE=$(printf '%s' "$LAST" | jq -r '.max_queued_queries')
check '.running_queries == 0' "queries are still running before the admission test"
manage PUT "$API/admission" '{"max_running_queries": 1}'
check '.action == "admission.set" and .changed == true' "admission.set envelope is wrong"
check '.result.max_running_queries == 1' "admission result does not report the new cap 1"
submit "$LONG_SQL"
wait_state running 60
FIRST_ID="$ID"
submit "$LONG_SQL"
SECOND_ID="$ID"
wait_state queued 20
echo "  second statement queued behind a cap of 1"
req USER2 DELETE "$API/statements/$SECOND_ID"
req USER2 DELETE "$API/statements/$FIRST_ID"
ID="$FIRST_ID"
poll 1000
wait_state cancelled 30
manage PUT "$API/admission" "$(jq -nc --argjson r "$ORIG_RUN" --argjson q "$ORIG_QUEUE" \
    '{max_running_queries:$r, max_queued_queries:$q}')"
req ADMIN GET "$API/admission"
expect_code 200 "GET /admission"
check ".max_running_queries == $ORIG_RUN and .max_queued_queries == $ORIG_QUEUE" "the original limits were not restored"
check '.source == "state"' "admission source is not state after a runtime change"
pass

STEP="7 write privilege"
psql_user2 "CREATE TABLE $CATALOG.$SCHEMA.smoke_t AS SELECT 1 AS x"
[ "$PSQL_RC" != 0 ] || fail "pgwire CREATE TABLE as user2 succeeded"
case "$PSQL_OUT" in *42501*) ;; *) fail "pgwire CREATE TABLE error is not SQLSTATE 42501: $PSQL_OUT" ;; esac
submit "CREATE TABLE $CATALOG.$SCHEMA.smoke_t AS SELECT 1 AS x"
wait_terminal
check '.state == "failed"' "API CREATE TABLE as user2 did not end failed"
check '.error.code == "forbidden"' "API CREATE TABLE error code is not forbidden"
pass

STEP="8 audit log"
req ADMIN GET "$API/audit?limit=50"
expect_code 200 "GET /audit"
for a in worker.cordon worker.drain worker.uncordon query.cancel admission.set; do
    check "any(.entries[]; .action == \"$a\" and .user == \"$ADMIN\")" "no audit entry $a by $ADMIN"
done
check "all(.entries[]; .user != \"$USER2\")" "the audit log holds an entry by $USER2"
check '.writable == true' "the audit log is not writable"
pass

STEP="9 MCP"
out=$(curl -sS -u "$ADMIN:$ARNEB_ADMIN_PASSWORD" -X POST "$MCP" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    -H 'MCP-Protocol-Version: 2025-06-18' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}') || fail "curl tools/list"
LAST="$out"
check '.result.tools | length == 25' "tools/list does not have exactly 25 tools"
mcp_tool ADMIN get_worker "$(jq -nc --arg id "$W1" '{id:$id}')"
check '.isError == false' "get_worker as admin returned isError"
check '.structuredContent.scheduling == "schedulable"' "get_worker does not report $W1 schedulable"
mcp_tool USER2 cordon_worker "$(jq -nc --arg id "$W1" '{id:$id}')"
check '.isError == true' "cordon_worker as user2 should have returned isError"
check '.structuredContent.error.code == "forbidden"' "cordon_worker as user2 code is not forbidden"
pass

if [ "${KEEP_CORDON:-}" = 1 ]; then
    STEP="9b keep a cordon for the restart check"
    manage POST "$API/workers/$W1/cordon"
    check '.result.scheduling == "cordoned"' "cordon of $W1 did not apply"
    echo "  $W1 left cordoned; restart the coordinator, then run with --after-restart"
    pass
fi
