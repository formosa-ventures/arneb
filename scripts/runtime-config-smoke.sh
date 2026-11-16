#!/usr/bin/env bash
# Runtime config smoke test (/api/v2 catalog-configs, access-rules, users,
# user tokens, settings, re-seed; MCP).
#
# Runs the acceptance scenario of the runtime-config change against a live
# distributed stack: a coordinator plus two workers with password auth,
# `admin_users = [<admin>]`, a persistent `[state] path`, the same
# `[cluster] secret` on every node (PG_URL carries a password, which the
# coordinator refuses to serve over unauthenticated cluster RPC), a rules file that
# lets user2 read the TPC-H tables, and a PostgreSQL database for the new
# catalog (the docker compose `postgres` service with the pg-tpch-seed
# `tpch_mixed` schema). Takes the environment of scripts/manage-smoke.sh plus
# PG_URL. Uses curl and jq for the API and psql for the pgwire checks.
# Prints `PASS <step>` per check and exits non-zero at the first
# `FAIL <step>`. Passwords, tokens and PG_URL are never printed, and never
# put on a command line (where `ps` shows them): curl gets credentials and
# bodies as a config on stdin, jq reads secrets from the environment, and
# psql gets its password through PGPASSWORD.
#
# Environment:
#   ARNEB_URL             Web port base URL, e.g. http://127.0.0.1:6432 (required)
#   ARNEB_PG              host:port of the pgwire port, e.g. 127.0.0.1:5442 (required)
#   ARNEB_ADMIN_USER / ARNEB_ADMIN_PASSWORD   a user in admin_users (required)
#   ARNEB_USER2 / ARNEB_PASSWORD2             a user without manage (required)
#   PG_URL                postgresql://user:password@host:port/db the coordinator
#                         and the workers can reach (required for the full run)
#   PG_URL_HOST           the same database as this host reaches it, for the
#                         expected row count (default PG_URL); e.g. when the
#                         nodes run in kind and PG_URL names a host only they resolve
#   ARNEB_PG_CATALOG      name of the catalog to create (default pg2)
#   ARNEB_PG_SCHEMA       schema of the PostgreSQL TPC-H tables (default tpch_mixed)
#   ARNEB_PG_TABLE        table to count across the workers (default customer)
#   ARNEB_SMOKE_USER      the runtime user to create (default erin)
#   ARNEB_SMOKE_THRESHOLD_MS  slow-query threshold to set (default 54321)
#   ARNEB_WORKER_LOGS     optional space-separated worker log files; when set,
#                         step 2 checks each shows the catalog sync and a task,
#                         and step 9 checks each shows a pull and a task written
#                         after step 7 (the lines step 7 saw are skipped; a log
#                         file step 7 did not see is read whole)
#   ARNEB_SMOKE_STATE     where the run keeps non-secret values for the
#                         restart check (default ${TMPDIR:-/tmp}/runtime-config-smoke.json)
#
# Usage:
#   scripts/runtime-config-smoke.sh                 steps 1 to 7
#   scripts/runtime-config-smoke.sh --after-restart steps 8 to 10, after
#       restarting the coordinator only and then one worker. Step 10 deletes
#       the smoke user and re-seeds the state from the config file, which
#       removes the catalog, the rules change and the threshold again, and
#       also resets every stored setting, including admission limits set by
#       `PUT /api/v2/admission` (for example by scripts/manage-smoke.sh).

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
if [ "$AFTER_RESTART" = 0 ]; then
    : "${PG_URL:?set PG_URL to postgresql://user:password@host:port/db}"
fi
PGCAT="${ARNEB_PG_CATALOG:-pg2}"
PGSCHEMA="${ARNEB_PG_SCHEMA:-tpch_mixed}"
PGTABLE="${ARNEB_PG_TABLE:-customer}"
NEWUSER="${ARNEB_SMOKE_USER:-erin}"
THRESHOLD="${ARNEB_SMOKE_THRESHOLD_MS:-54321}"
SMOKE_STATE="${ARNEB_SMOKE_STATE:-${TMPDIR:-/tmp}/runtime-config-smoke.json}"
WORKER_LOGS=()
if [ -n "${ARNEB_WORKER_LOGS:-}" ]; then read -r -a WORKER_LOGS <<<"$ARNEB_WORKER_LOGS"; fi
API="${ARNEB_URL%/}/api/v2"
MCP="${ARNEB_URL%/}/mcp"
ADMIN="$ARNEB_ADMIN_USER"
USER2="$ARNEB_USER2"
PG_HOST="${ARNEB_PG%:*}"
PG_PORT="${ARNEB_PG##*:}"
FQ="$PGCAT.$PGSCHEMA.$PGTABLE"
COUNT_SQL="SELECT count(*) FROM $FQ"

STEP=""
LAST=""
CODE=""
RID=0
TOKEN=""
NEW_PW=""
pass() { echo "PASS $STEP"; }
fail() {
    echo "FAIL $STEP: $*"
    [ -n "$LAST" ] && echo "  last response (HTTP ${CODE:-?}): $(printf '%s' "$LAST" | redact | head -c 2000)"
    exit 1
}
# redact: strip every secret this run knows from text it is about to print.
redact() {
    local s
    s=$(cat)
    for secret in "$ARNEB_ADMIN_PASSWORD" "$ARNEB_PASSWORD2" "${PG_URL:-}" "${PG_URL_HOST:-}" "$TOKEN" "$NEW_PW"; do
        [ -n "$secret" ] && s="${s//"$secret"/<secret>}"
    done
    printf '%s' "$s"
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}

# cfgq <text>: <text> as a double-quoted curl config value.
cfgq() {
    local v="${1//\\/\\\\}"
    v="${v//\"/\\\"}"
    printf '"%s"' "$v"
}

# curl_cfg <ADMIN|USER2|TOKEN> [body]: a curl config carrying the credentials
# (Basic, or the smoke user's API token as a bearer) and the body, for
# `curl -K -`. printf is a shell builtin, so none of it reaches an argv.
curl_cfg() {
    case "$1" in
        ADMIN) printf 'user = %s\n' "$(cfgq "$ADMIN:$ARNEB_ADMIN_PASSWORD")" ;;
        USER2) printf 'user = %s\n' "$(cfgq "$USER2:$ARNEB_PASSWORD2")" ;;
        TOKEN) printf 'header = %s\n' "$(cfgq "Authorization: Bearer $TOKEN")" ;;
        *) fail "curl_cfg: unknown identity $1" ;;
    esac
    if [ $# -ge 2 ]; then
        printf 'data-raw = %s\n' "$(cfgq "$2")"
    fi
}

# req <ADMIN|USER2|TOKEN> <method> <url> [json-body]: body to $LAST, status to
# $CODE.
req() {
    local out
    if [ $# -ge 4 ]; then
        out=$(curl_cfg "$1" "$4" | curl -sS -K - -w '\n%{http_code}' -X "$2" "$3" \
            -H 'Content-Type: application/json') || fail "curl $2 $3"
    else
        out=$(curl_cfg "$1" | curl -sS -K - -w '\n%{http_code}' -X "$2" "$3") || fail "curl $2 $3"
    fi
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
}

# mcp_post <ADMIN|USER2> <json-rpc-body>: POST /mcp; body to $LAST, status to
# $CODE.
mcp_post() {
    local out
    out=$(curl_cfg "$1" "$2" | curl -sS -K - -w '\n%{http_code}' -X POST "$MCP" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json, text/event-stream' \
        -H 'MCP-Protocol-Version: 2025-06-18') || fail "curl POST /mcp"
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
}

expect_code() { [ "$CODE" = "$1" ] || fail "$2 returned HTTP $CODE, want $1"; }

# manage <method> <url> [json-body]: a real manage call as the admin that must
# answer 200 with a non-dry envelope.
manage() {
    req ADMIN "$@"
    expect_code 200 "$1 $2"
    check '.dry_run == false' "$1 $2 answered a dry run"
}

# preview <method> <url> [json-body]: the same call as a dry run.
preview() {
    local m="$1" path="$2" u
    shift 2
    case "$path" in *\?*) u="$path&dry_run=true" ;; *) u="$path?dry_run=true" ;; esac
    req ADMIN "$m" "$u" "$@"
    expect_code 200 "dry-run $m $path"
    check '.dry_run == true and ((has("result") | not) or .result == null)' "dry-run $m $path envelope is wrong"
}

# forbidden <method> <url> [json-body]: user2 must get 403 forbidden.
forbidden() {
    req USER2 "$@"
    expect_code 403 "$USER2 $1 $2"
    check '.error.code == "forbidden"' "$USER2 $1 $2 error code is not forbidden"
}

# statement <ADMIN|USER2|TOKEN> <sql>: run it through POST /statements and
# wait for a terminal state; $LAST is the final status.
statement() {
    local who="$1" id state deadline
    req "$who" POST "$API/statements?wait_ms=5000" "$(jq -nc --arg sql "$2" '{sql:$sql}')"
    case "$CODE" in 200 | 201) ;; *) fail "submit as $who returned HTTP $CODE" ;; esac
    id=$(printf '%s' "$LAST" | jq -r '.id')
    deadline=$(( $(date +%s) + 300 ))
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        case "$state" in finished | failed | cancelled) return 0 ;; esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $id still $state after 300 s"
        req "$who" GET "$API/statements/$id?wait_ms=5000"
        expect_code 200 "GET /statements/$id"
    done
}

# first_cell <ADMIN|USER2|TOKEN>: the single value of the finished one-row
# statement in $LAST, as text.
first_cell() {
    local rows id
    rows=$(printf '%s' "$LAST" | jq -c '.first_page.rows // empty')
    if [ -z "$rows" ]; then
        id=$(printf '%s' "$LAST" | jq -r '.id')
        req "$1" GET "$API/statements/$id/results?cursor=0"
        expect_code 200 "GET /statements/$id/results"
        rows=$(printf '%s' "$LAST" | jq -c '.rows')
    fi
    printf '%s' "$rows" | jq -r '.[0][0] | tostring'
}

# psql_as <user> <password> <sql>: output to $PSQL_OUT, exit status to $PSQL_RC.
PSQL_OUT=""
PSQL_RC=0
psql_as() {
    PSQL_RC=0
    PSQL_OUT=$(PGPASSWORD="$2" PGCONNECT_TIMEOUT=10 psql -X -tAq \
        -v VERBOSITY=sqlstate -h "$PG_HOST" -p "$PG_PORT" -U "$1" -d arneb \
        -c "$3" 2>&1) || PSQL_RC=$?
    PSQL_OUT=$(printf '%s' "$PSQL_OUT" | redact)
}

# mcp_tool <ADMIN|USER2> <name> [args-json]: tools/call, $LAST = result.
mcp_tool() {
    local targs="${3:-}"
    [ -n "$targs" ] || targs='{}'
    RID=$(( RID + 1 ))
    mcp_post "$1" "$(jq -nc --argjson id "$RID" --arg n "$2" --argjson a "$targs" \
        '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:$n, arguments:$a}}')"
    expect_code 200 "tools/call $2"
    check '.error == null' "tools/call $2 returned a JSON-RPC error"
    LAST=$(printf '%s' "$LAST" | jq -c '.result')
}

# log_has <file> <extended-regex>: the worker log (ANSI colors stripped) has a
# matching line. No `grep -q`: an early exit would SIGPIPE sed, and pipefail
# would turn a match into a failure.
log_has() {
    log_has_after "$1" 0 "$2"
}

# log_has_after <file> <lines> <extended-regex>: the same, ignoring the first
# <lines> lines.
log_has_after() {
    tail -n "+$(( $2 + 1 ))" "$1" | sed 's/\x1b\[[0-9;]*m//g' | grep -E "$3" >/dev/null
}

# wait_workers <seconds>: wait until both workers are registered and
# schedulable.
wait_workers() {
    local deadline=$(( $(date +%s) + $1 ))
    while :; do
        req ADMIN GET "$API/workers"
        if [ "$CODE" = 200 ] && printf '%s' "$LAST" |
            jq -e '[.workers[] | select(.registered == true and .scheduling == "schedulable")] | length >= 2' >/dev/null 2>&1; then
            return 0
        fi
        [ "$(date +%s)" -lt "$deadline" ] || fail "fewer than 2 schedulable workers after $1 s"
        sleep 1
    done
}

# count_check: COUNT_SQL over pgwire as the admin must be distributed (EXPLAIN
# shows an Exchange) and equal $EXPECTED.
count_check() {
    psql_as "$ADMIN" "$ARNEB_ADMIN_PASSWORD" "EXPLAIN $COUNT_SQL"
    [ "$PSQL_RC" = 0 ] || fail "EXPLAIN failed: $PSQL_OUT"
    case "$PSQL_OUT" in *Exchange*) ;; *) fail "EXPLAIN shows no remote fragment (Exchange): $PSQL_OUT" ;; esac
    psql_as "$ADMIN" "$ARNEB_ADMIN_PASSWORD" "$COUNT_SQL"
    [ "$PSQL_RC" = 0 ] || fail "$COUNT_SQL failed: $PSQL_OUT"
    [ "$PSQL_OUT" = "$EXPECTED" ] || fail "$COUNT_SQL returned $PSQL_OUT, want $EXPECTED"
    echo "  $FQ: $PSQL_OUT rows, distributed"
}

after_restart() {
    [ -r "$SMOKE_STATE" ] || { STEP="8 setup"; fail "no $SMOKE_STATE: run the full smoke first"; }
    EXPECTED=$(jq -r '.expected' "$SMOKE_STATE")
    THRESHOLD=$(jq -r '.threshold' "$SMOKE_STATE")

    STEP="8 runtime config survives a coordinator restart"
    req ADMIN GET "$API/catalog-configs/$PGCAT"
    expect_code 200 "GET /catalog-configs/$PGCAT"
    check '.from_config == false and .spec.type == "postgresql"' "$PGCAT is not the runtime postgresql catalog"
    req ADMIN GET "$API/access-rules"
    expect_code 200 "GET /access-rules"
    check "any(.rules.catalogs[]?; .user == \"$USER2\" and .catalog == \"$PGCAT\" and .allow == \"none\")" \
        "the rule that denies $USER2 $PGCAT is gone"
    check '.from_config == false' "the rules report from_config true after a runtime PUT"
    req ADMIN GET "$API/users"
    expect_code 200 "GET /users"
    check "any(.users[]; .name == \"$NEWUSER\" and .from_config == false)" "user $NEWUSER is gone"
    req ADMIN GET "$API/settings"
    expect_code 200 "GET /settings"
    check ".slow_query_threshold_ms == $THRESHOLD and .source == \"state\"" "the threshold $THRESHOLD did not survive"
    psql_as "$USER2" "$ARNEB_PASSWORD2" "$COUNT_SQL"
    [ "$PSQL_RC" != 0 ] || fail "$USER2 can read $PGCAT after the restart"
    case "$PSQL_OUT" in *42501*) ;; *) fail "$USER2 refusal is not SQLSTATE 42501: $PSQL_OUT" ;; esac
    pass

    STEP="9 a restarted worker pulls the catalog"
    wait_workers 90
    count_check
    local skip
    for log in ${WORKER_LOGS[@]+"${WORKER_LOGS[@]}"}; do
        # Only lines written after step 7: the coordinator restart (new epoch)
        # and the worker restart both come later, so each must pull again.
        skip=$(jq -r --arg p "$log" '.logs[$p] // 0' "$SMOKE_STATE")
        log_has_after "$log" "$skip" 'catalogs pulled from the coordinator' \
            || fail "$log shows no catalog pull after line $skip (step 7)"
        log_has_after "$log" "$skip" 'executing task' || fail "$log ran no task after line $skip (step 7)"
        echo "  $log: a pull and a task after line $skip"
    done
    pass

    STEP="10 re-seed restores the config"
    manage DELETE "$API/users/$NEWUSER"
    check '.action == "user.delete" and .changed == true' "user.delete envelope is wrong"
    preview POST "$API/state/reseed"
    check '.action == "state.reseed"' "re-seed preview envelope is wrong"
    echo "  preview: $(printf '%s' "$LAST" | jq -c '{summary, warning}' | redact)"
    manage POST "$API/state/reseed"
    check '.action == "state.reseed" and .changed == true' "re-seed envelope is wrong"
    req ADMIN GET "$API/catalog-configs/$PGCAT"
    expect_code 404 "GET /catalog-configs/$PGCAT after the re-seed"
    req ADMIN GET "$API/settings"
    check '.source == "config"' "the threshold source is not config after the re-seed"
    req ADMIN GET "$API/access-rules"
    check "(.rules == null and .from_config == false) or .from_config == true" "the rules are not the config's after the re-seed"
    req ADMIN GET "$API/audit?limit=20"
    check "any(.entries[]; .action == \"state.reseed\" and .user == \"$ADMIN\")" "no state.reseed audit entry"
    rm -f "$SMOKE_STATE"
    pass
}

if [ "$AFTER_RESTART" = 1 ]; then
    after_restart
    exit 0
fi

STEP="0 setup"
# psql gets PG_URL_HOST without its password, and the password through PGPASSWORD.
PG_LOCAL_URL="${PG_URL_HOST:-$PG_URL}"
PG_REST="${PG_LOCAL_URL#*://}"
PG_USERINFO=""
case "$PG_REST" in *@*) PG_USERINFO="${PG_REST%%@*}"; PG_REST="${PG_REST#*@}" ;; esac
PG_PASS=""
case "$PG_USERINFO" in *:*) PG_PASS="${PG_USERINFO#*:}"; PG_USERINFO="${PG_USERINFO%%:*}" ;; esac
PG_PASS=$(printf '%b' "${PG_PASS//%/\\x}")   # percent-decode
PG_NOPW_URL="${PG_LOCAL_URL%%://*}://${PG_USERINFO:+$PG_USERINFO@}$PG_REST"
EXPECTED=$(PGPASSWORD="$PG_PASS" PGCONNECT_TIMEOUT=10 psql -X -tAq "$PG_NOPW_URL" \
    -c "SELECT count(*) FROM $PGSCHEMA.$PGTABLE" 2>&1 | redact) \
    || fail "cannot count $PGSCHEMA.$PGTABLE through PG_URL_HOST or PG_URL: $EXPECTED"
NEW_PW=$(openssl rand -hex 16)
req ADMIN GET "$API/whoami"
expect_code 200 "admin whoami"
check '.privileges.manage == true' "$ADMIN has no manage privilege (is $ADMIN in admin_users?)"
req ADMIN GET "$API/catalog-configs/$PGCAT"
expect_code 404 "GET /catalog-configs/$PGCAT (leftover from an earlier run?)"
wait_workers 60
pass

STEP="1 create a postgresql catalog"
SPEC=$(PG_URL="$PG_URL" jq -nc --arg n "$PGCAT" --arg s "$PGSCHEMA" \
    '{name:$n, type:"postgresql", connection_url:env.PG_URL, default_schema:$s, split_min_rows:100000}')
preview POST "$API/catalog-configs" "$SPEC"
check '.action == "catalog.create" and .target == "'"$PGCAT"'"' "create preview envelope is wrong"
req ADMIN GET "$API/catalogs"
check "all(.items[]; .name != \"$PGCAT\")" "a dry run created $PGCAT"
manage POST "$API/catalog-configs" "$SPEC"
check '.action == "catalog.create" and .changed == true' "create envelope is wrong"
check '.result.workers | length >= 2 and all(.[]; .synced == true)' "not every worker synced $PGCAT"
GENERATION=$(printf '%s' "$LAST" | jq -r '.result.generation')
case "$LAST" in *"$PG_URL"*) fail "the create answer carries the raw connection_url" ;; esac
req ADMIN GET "$API/catalogs"
expect_code 200 "GET /catalogs"
check "any(.items[]; .name == \"$PGCAT\")" "/api/v2/catalogs does not list $PGCAT"
req ADMIN GET "$API/catalog-configs/$PGCAT"
expect_code 200 "GET /catalog-configs/$PGCAT"
check '.spec.connection_url | test("<redacted>")' "GET /catalog-configs/$PGCAT does not redact the password"
echo "  $PGCAT at generation $GENERATION, synced to every worker"
pass

STEP="2 the catalog is scanned across both workers"
count_check
for log in ${WORKER_LOGS[@]+"${WORKER_LOGS[@]}"}; do
    log_has "$log" "registered catalog catalog=$PGCAT .*generation=$GENERATION" \
        || fail "$log shows no sync of $PGCAT at generation $GENERATION"
    log_has "$log" 'executing task' || fail "$log ran no task"
done
[ "${#WORKER_LOGS[@]}" -gt 0 ] && echo "  every worker log shows the sync of $PGCAT and a task"
pass

STEP="3 rules that deny $USER2 the catalog apply without a restart"
req ADMIN GET "$API/access-rules"
expect_code 200 "GET /access-rules"
# Deny user2 the new catalog; let the smoke user run statements and read it.
RULES=$(printf '%s' "$LAST" | jq -c --arg u2 "$USER2" --arg c "$PGCAT" --arg n "$NEWUSER" '
    (.rules // {})
    | .catalogs = ([{user:$u2, catalog:$c, allow:"none"}] + (.catalogs // [{allow:"all"}]))
    | if has("tables") then .tables = [{user:$n, catalog:$c, privileges:["SELECT"]}] + .tables else . end
    | if has("queries") then .queries = [{user:$n, allow:["execute"]}] + .queries else . end')
psql_as "$USER2" "$ARNEB_PASSWORD2" "$COUNT_SQL"
[ "$PSQL_RC" = 0 ] || fail "$USER2 cannot read $FQ before the rules change: $PSQL_OUT"
preview PUT "$API/access-rules" "$RULES"
manage PUT "$API/access-rules" "$RULES"
check '.action == "access_rules.set" and .changed == true' "access_rules.set envelope is wrong"
statement USER2 "$COUNT_SQL"
check '.state == "failed" and .error.code == "forbidden"' "$USER2 statement on $PGCAT is not forbidden"
req USER2 GET "$API/catalogs"
check "all(.items[]; .name != \"$PGCAT\")" "/api/v2/catalogs still lists $PGCAT to $USER2"
psql_as "$USER2" "$ARNEB_PASSWORD2" "$COUNT_SQL"
[ "$PSQL_RC" != 0 ] || fail "pgwire $COUNT_SQL as $USER2 succeeded"
case "$PSQL_OUT" in *42501*) ;; *) fail "pgwire refusal is not SQLSTATE 42501: $PSQL_OUT" ;; esac
pass

STEP="4 a runtime user and a token for them"
manage POST "$API/users" "$(NEW_PW="$NEW_PW" jq -nc --arg n "$NEWUSER" '{name:$n, password:env.NEW_PW}')"
check '.action == "user.create" and .changed == true' "user.create envelope is wrong"
manage POST "$API/users/$NEWUSER/tokens" '{"name":"smoke"}'
check '.action == "token.issue" and .target == "'"$NEWUSER"'"' "token.issue envelope is wrong"
TOKEN=$(printf '%s' "$LAST" | jq -r '.result.token')
TOKEN_ID=$(printf '%s' "$LAST" | jq -r '.result.id')
[ -n "$TOKEN" ] && [ "$TOKEN" != null ] || fail "no token in the issue answer"
req TOKEN GET "$API/whoami"
expect_code 200 "whoami with $NEWUSER's token"
check '.user == "'"$NEWUSER"'" and .via == "token"' "the token does not authenticate $NEWUSER"
statement TOKEN "$COUNT_SQL"
check '.state == "finished"' "$NEWUSER's API statement did not finish"
[ "$(first_cell TOKEN)" = "$EXPECTED" ] || fail "$NEWUSER's API count is not $EXPECTED"
psql_as "$NEWUSER" "$TOKEN" "$COUNT_SQL"
[ "$PSQL_RC" = 0 ] || fail "pgwire as $NEWUSER with the token failed: $PSQL_OUT"
[ "$PSQL_OUT" = "$EXPECTED" ] || fail "pgwire as $NEWUSER returned $PSQL_OUT, want $EXPECTED"
manage DELETE "$API/users/$NEWUSER/tokens/$TOKEN_ID"
check '.action == "token.revoke" and .changed == true' "token.revoke envelope is wrong"
req TOKEN GET "$API/whoami"
expect_code 401 "whoami with the revoked token"
psql_as "$NEWUSER" "$TOKEN" "SELECT 1"
[ "$PSQL_RC" != 0 ] || fail "pgwire accepted the revoked token"
case "$PSQL_OUT" in *28P01* | *"password authentication failed"*) ;; *) fail "pgwire refusal of the revoked token is not an authentication failure: $PSQL_OUT" ;; esac
pass

STEP="5 $USER2 may not change the runtime config"
DENIED_SPEC=$(PG_URL="$PG_URL" jq -nc '{name:"smoke_denied", type:"postgresql", connection_url:env.PG_URL}')
forbidden POST "$API/catalog-configs" "$DENIED_SPEC"
forbidden POST "$API/catalog-configs?dry_run=true" "$DENIED_SPEC"
forbidden PUT "$API/access-rules" '{}'
forbidden DELETE "$API/access-rules"
forbidden POST "$API/users" '{"name":"smoke_denied","password":"not-a-real-password"}'
forbidden POST "$API/users/$NEWUSER/tokens" '{}'
forbidden PATCH "$API/settings" '{"slow_query_threshold_ms": 1}'
mcp_post ADMIN '{"jsonrpc":"2.0","id":0,"method":"tools/list","params":{}}'
expect_code 200 "tools/list"
check '.result.tools | length == 44' "tools/list does not have exactly 44 tools"
mcp_tool USER2 create_user "$(jq -nc --arg n smoke_denied '{name:$n, password:"not-a-real-password"}')"
check '.isError == true and .structuredContent.error.code == "forbidden"' "MCP create_user as $USER2 is not forbidden"
mcp_tool USER2 set_access_rules '{"rules":{}}'
check '.isError == true and .structuredContent.error.code == "forbidden"' "MCP set_access_rules as $USER2 is not forbidden"
pass

STEP="6 change the slow-query threshold"
req ADMIN GET "$API/settings"
expect_code 200 "GET /settings"
check ".slow_query_threshold_ms != $THRESHOLD" "the threshold is already $THRESHOLD (pick another ARNEB_SMOKE_THRESHOLD_MS)"
preview PATCH "$API/settings" "{\"slow_query_threshold_ms\": $THRESHOLD}"
manage PATCH "$API/settings" "{\"slow_query_threshold_ms\": $THRESHOLD}"
check '.action == "settings.update" and .changed == true' "settings.update envelope is wrong"
req ADMIN GET "$API/settings"
check ".slow_query_threshold_ms == $THRESHOLD and .source == \"state\"" "GET /settings does not report $THRESHOLD from the state"
mcp_tool ADMIN get_settings
check ".isError == false and .structuredContent.slow_query_threshold_ms == $THRESHOLD" "MCP get_settings disagrees"
pass

STEP="7 the audit log records every change, without secrets"
req ADMIN GET "$API/audit?limit=100"
expect_code 200 "GET /audit"
for a in "catalog.create:$PGCAT" "access_rules.set:" "user.create:$NEWUSER" \
    "token.issue:$NEWUSER" "token.revoke:$NEWUSER" "settings.update:"; do
    action="${a%%:*}" target="${a#*:}"
    if [ -n "$target" ]; then
        check "any(.entries[]; .action == \"$action\" and .target == \"$target\" and .user == \"$ADMIN\" and .via == \"basic\" and .outcome == \"ok\")" \
            "no audit entry $action on $target by $ADMIN"
    else
        check "any(.entries[]; .action == \"$action\" and .user == \"$ADMIN\" and .outcome == \"ok\")" \
            "no audit entry $action by $ADMIN"
    fi
done
check "all(.entries[]; .user != \"$USER2\")" "the audit log holds an entry by $USER2 (refusals are not audited)"
# The userinfo of PG_URL, as `user:password@`, when it carries a password.
PG_SECRET="${PG_URL#*://}"
case "$PG_SECRET" in *@*) PG_SECRET="${PG_SECRET%%@*}@" ;; *) PG_SECRET="" ;; esac
case "$PG_SECRET" in *:*) ;; *) PG_SECRET="$PG_URL" ;; esac
for secret in "$NEW_PW" "$TOKEN" "$PG_URL" "$ARNEB_ADMIN_PASSWORD" "$PG_SECRET"; do
    case "$LAST" in *"$secret"*) fail "the audit log contains a password, token or connection secret" ;; esac
done
umask 077
LOG_LINES='{}'
for log in ${WORKER_LOGS[@]+"${WORKER_LOGS[@]}"}; do
    LOG_LINES=$(printf '%s' "$LOG_LINES" | jq -c --arg p "$log" --argjson n "$(wc -l <"$log" | tr -d ' ')" '.[$p] = $n')
done
jq -nc --arg e "$EXPECTED" --argjson t "$THRESHOLD" --argjson l "$LOG_LINES" \
    '{expected:$e, threshold:$t, logs:$l}' >"$SMOKE_STATE"
echo "  restart the coordinator, then one worker, then run with --after-restart"
pass
