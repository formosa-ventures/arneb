#!/usr/bin/env bash
# MCP endpoint smoke test (POST /mcp, JSON-RPC over streamable HTTP).
#
# Drives the remote MCP tools against a live coordinator (or standalone)
# serving the hive TPC-H tables. Uses curl and jq only. Prints `PASS <step>`
# per check and exits non-zero at the first `FAIL <step>`.
#
# Environment:
#   ARNEB_URL        Web port base URL, e.g. http://127.0.0.1:6432 (required)
#   ARNEB_TOKEN      API token of the first user, or
#   ARNEB_USER / ARNEB_PASSWORD   that user's password (HTTP Basic)
#   ARNEB_TOKEN2     credentials of a second user (ownership check), or
#   ARNEB_USER2 / ARNEB_PASSWORD2
#   ARNEB_CATALOG    TPC-H catalog (default datalake)
#   ARNEB_SCHEMA     TPC-H schema (default tpch)
#   ARNEB_LONG_SQL   the long statement to cancel (default: 3-way self-join
#                    of lineitem)
#   ARNEB_SMOKE_TIMEOUT_SECS  per-statement timeout (default 900)
#
# Step 10 creates an API token for the first user, uses it and revokes it;
# the token itself is never printed.

set -euo pipefail

: "${ARNEB_URL:?set ARNEB_URL to the Web port, e.g. http://127.0.0.1:6432}"
CATALOG="${ARNEB_CATALOG:-datalake}"
SCHEMA="${ARNEB_SCHEMA:-tpch}"
TIMEOUT="${ARNEB_SMOKE_TIMEOUT_SECS:-900}"
LONG_SQL="${ARNEB_LONG_SQL:-SELECT count(*) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey JOIN lineitem c ON b.l_orderkey = c.l_orderkey}"
MCP="${ARNEB_URL%/}/mcp"
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
CODE=""
RID=0
pass() { echo "PASS $STEP"; }
fail() {
    echo "FAIL $STEP: $*"
    [ -n "$LAST" ] && echo "  last response: $(printf '%s' "$LAST" | head -c 2000)"
    exit 1
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}

NEW_TOKEN=""
# post <AUTH1|AUTH2|NEW|NONE> <json-body>: raw response body to $LAST, HTTP
# status to $CODE. NEW is the token minted in step 10 (never printed).
post() {
    local out auth=()
    case "$1" in
        AUTH1) auth=("${AUTH1[@]}") ;;
        AUTH2) auth=("${AUTH2[@]}") ;;
        NEW) auth=(-H "Authorization: Bearer $NEW_TOKEN") ;;
    esac
    out=$(curl -sS -w '\n%{http_code}' ${auth[@]+"${auth[@]}"} -X POST "$MCP" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json, text/event-stream' \
        -H 'MCP-Protocol-Version: 2025-06-18' \
        -d "$2") || fail "curl POST /mcp"
    CODE="${out##*$'\n'}"
    LAST="${out%$'\n'*}"
}

# rpc <AUTH1|AUTH2|NEW> <method> <params-json>: JSON-RPC call, $LAST = response.
rpc() {
    RID=$(( RID + 1 ))
    post "$1" "$(jq -nc --argjson id "$RID" --arg m "$2" --argjson p "$3" \
        '{jsonrpc:"2.0", id:$id, method:$m, params:$p}')"
    [ "$CODE" = 200 ] || fail "$2 returned HTTP $CODE"
    check '.error == null' "JSON-RPC error"
}

# tool <AUTH1|AUTH2|NEW> <name> [args-json]: tools/call; $LAST = the full result.
# Use `sc` to reduce it to the structuredContent.
tool() {
    local targs="${3:-}"
    [ -n "$targs" ] || targs='{}'
    rpc "$1" tools/call "$(jq -nc --arg n "$2" --argjson a "$targs" '{name:$n, arguments:$a}')"
    LAST=$(printf '%s' "$LAST" | jq -c '.result')
}
tool_ok() { # tool call that must not be an error; $LAST = structuredContent
    tool "$@"
    check '.isError == false' "$2 returned isError"
    LAST=$(printf '%s' "$LAST" | jq -c '.structuredContent')
}
tool_err() { # tool call that must be an error; $LAST = structuredContent
    tool "$@"
    check '.isError == true' "$2 should have returned isError"
    LAST=$(printf '%s' "$LAST" | jq -c '.structuredContent')
}

# submit <sql> [max_rows] [wait_ms]: submit_query as user 1, sets $ID.
submit() {
    local args
    args=$(jq -nc --arg sql "$1" --arg c "$CATALOG" --arg s "$SCHEMA" --arg m "${2:-}" --arg w "${3:-2000}" \
        '{sql:$sql, catalog:$c, schema:$s, wait_ms:($w|tonumber)} + (if $m == "" then {} else {max_rows:($m|tonumber)} end)')
    tool_ok AUTH1 submit_query "$args"
    ID=$(printf '%s' "$LAST" | jq -r '.id')
}

# wait_terminal: poll $ID with get_query_status until finished, failed or cancelled.
wait_terminal() {
    local deadline=$(( $(date +%s) + TIMEOUT )) state
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        case "$state" in finished|failed|cancelled) return 0 ;; esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $ID still $state after ${TIMEOUT}s"
        tool_ok AUTH1 get_query_status "$(jq -nc --arg id "$ID" '{id:$id, wait_ms:5000}')"
    done
}

STEP="1 initialize and tools"
rpc AUTH1 initialize '{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"mcp-smoke","version":"1"}}'
check '.result.serverInfo.name == "arneb"' "serverInfo.name is not arneb"
rpc AUTH1 tools/list '{}'
check '.result.tools | length == 18' "tools/list does not have exactly 18 tools"
for t in whoami list_catalogs list_schemas list_tables describe_table sample_table explain_query \
    submit_query get_query_status fetch_results cancel_query list_queries get_query get_cluster \
    list_workers list_tokens create_token revoke_token; do
    check "any(.result.tools[]; .name == \"$t\")" "tool $t missing"
done
tool_ok AUTH1 whoami
check '.user | type == "string" and length > 0' "whoami has no user"
check '.auth == "password"' "whoami auth is not password"
check '.via == "token" or .via == "basic"' "whoami via is not token or basic"
USER1=$(printf '%s' "$LAST" | jq -r '.user')
echo "  whoami: $(printf '%s' "$LAST" | jq -c '{user, via, role}')"
pass

STEP="2 no credentials"
post NONE '{"jsonrpc":"2.0","id":99,"method":"tools/list","params":{}}'
[ "$CODE" = 401 ] || fail "no credentials got HTTP $CODE, want 401"
post NONE '{"jsonrpc":"2.0","id":98,"method":"tools/call","params":{"name":"whoami","arguments":{}}}'
[ "$CODE" = 401 ] || fail "whoami without credentials got HTTP $CODE, want 401"
LAST=""
pass

STEP="3 discovery"
tool_ok AUTH1 list_catalogs
check "any(.items[]; .name == \"$CATALOG\")" "catalog $CATALOG not listed"
tool_ok AUTH1 list_tables "$(jq -nc --arg c "$CATALOG" --arg s "$SCHEMA" '{catalog:$c, schema:$s}')"
check 'any(.items[]; .name == "lineitem")' "lineitem not listed"
tool_ok AUTH1 describe_table "$(jq -nc --arg c "$CATALOG" --arg s "$SCHEMA" '{catalog:$c, schema:$s, table:"lineitem"}')"
check 'any(.columns[]; .name == "l_quantity")' "l_quantity not described"
tool_ok AUTH1 sample_table "$(jq -nc --arg c "$CATALOG" --arg s "$SCHEMA" '{catalog:$c, schema:$s, table:"nation", rows:5}')"
ID=$(printf '%s' "$LAST" | jq -r '.id')
wait_terminal
check '.state == "finished"' "sample did not finish"
if printf '%s' "$LAST" | jq -e '.first_page' >/dev/null; then
    check '.first_page.rows | length == 5' "sample did not return 5 rows"
else
    tool_ok AUTH1 fetch_results "$(jq -nc --arg id "$ID" '{id:$id}')"
    check '.rows | length == 5' "sample did not return 5 rows"
fi
pass

STEP="4 explain and submit Q6"
tool AUTH1 explain_query "$(jq -nc --arg sql "$Q6" --arg c "$CATALOG" --arg s "$SCHEMA" '{sql:$sql, catalog:$c, schema:$s}')"
check '.isError == false' "explain_query returned isError"
# Text-only clients see the plan: the text block is the structured JSON.
check '(.content[0].text | fromjson) == .structuredContent' "explain text is not the structured JSON"
LAST=$(printf '%s' "$LAST" | jq -c '.structuredContent')
check '.plan | length > 0' "empty plan"
submit "$Q6"
wait_terminal
check '.state == "finished"' "Q6 did not finish"
if ! printf '%s' "$LAST" | jq -e '.first_page.rows' >/dev/null; then
    tool_ok AUTH1 fetch_results "$(jq -nc --arg id "$ID" '{id:$id}')"
    check '.rows | length == 1' "Q6 did not return one row"
else
    check '.first_page.rows | length == 1' "Q6 did not return one row"
fi
echo "  revenue=$(printf '%s' "$LAST" | jq -c '(.first_page.rows // .rows)[0][0]')"
pass

STEP="5 truncation"
submit "SELECT * FROM lineitem" 100
wait_terminal
check '.state == "finished"' "SELECT * did not finish"
check '.truncated == true' "not truncated"
check '.total_rows == null' "total_rows is not null for a truncated result"
ROWS=0
CURSOR=0
PAGES=0
while [ -n "$CURSOR" ] && [ "$CURSOR" != null ]; do
    tool_ok AUTH1 fetch_results "$(jq -nc --arg id "$ID" --argjson c "$CURSOR" '{id:$id, cursor:$c, max_bytes:8192}')"
    ROWS=$(( ROWS + $(printf '%s' "$LAST" | jq '.rows | length') ))
    CURSOR=$(printf '%s' "$LAST" | jq -r '.next_cursor')
    PAGES=$(( PAGES + 1 ))
    [ "$PAGES" -le 200 ] || fail "too many pages"
done
LAST=""
[ "$ROWS" = 100 ] || fail "fetched $ROWS rows, want 100"
echo "  rows=$ROWS pages=$PAGES"
OWNED_ID="$ID"
pass

STEP="6 bad column"
submit "SELECT l_quantiy FROM lineitem"
wait_terminal
if printf '%s' "$LAST" | jq -e '.state == "failed"' >/dev/null; then
    :
else
    fail "bad column did not end failed"
fi
check '.error.code == "column_not_found"' "code is not column_not_found"
check '.error.location != null' "no location"
check 'any(.error.candidates[]; . == "l_quantity")' "candidates lack l_quantity"
echo "  error: $(printf '%s' "$LAST" | jq -c '.error | {code, location, candidates}')"
pass

STEP="7 long statement cancel"
submit "$LONG_SQL" "" 200
deadline=$(( $(date +%s) + 60 ))
while :; do
    state=$(printf '%s' "$LAST" | jq -r '.state')
    [ "$state" = running ] && break
    [ "$state" = queued ] || fail "long statement is $state, not running"
    [ "$(date +%s)" -lt "$deadline" ] || fail "long statement never running"
    tool_ok AUTH1 get_query_status "$(jq -nc --arg id "$ID" '{id:$id, wait_ms:200}')"
done
echo "  observed running: id=$ID"
LONG_ID="$ID"
tool_ok AUTH1 cancel_query "$(jq -nc --arg id "$ID" '{id:$id}')"
check '.state == "cancelled"' "cancel did not answer cancelled"
tool_ok AUTH1 get_query_status "$(jq -nc --arg id "$ID" '{id:$id}')"
check '.state == "cancelled"' "status is not cancelled"
pass

STEP="8 history and cluster"
tool_ok AUTH1 list_queries '{"min_elapsed_ms":0,"limit":5}'
check '.queries | length >= 1 and length <= 5' "list_queries did not return 1 to 5 entries"
check '.queries[0].query_id == "'"$LONG_ID"'"' "newest query is not the cancelled long statement"
# shellcheck disable=SC2016
check '[.queries[].created_ms] as $t | $t == ($t | sort | reverse)' "queries are not newest first"
# Every reported state is a filter, and it keeps exactly that state.
for st in planning finishing cancelled; do
    tool_ok AUTH1 list_queries "$(jq -nc --arg s "$st" '{state:$s, limit:5}')"
    check '[.queries[].state] | all(. == "'"$st"'")' "state filter $st kept other states"
done
tool_ok AUTH1 get_cluster
check '.worker_count == 2' "get_cluster does not report 2 workers"
tool_ok AUTH1 list_workers
check '.workers | length == 2' "list_workers does not list 2 workers"
pass

STEP="9 ownership"
tool_err AUTH2 get_query_status "$(jq -nc --arg id "$OWNED_ID" '{id:$id}')"
check '.error.code == "not_found"' "code is not not_found"
pass

STEP="10 token flow"
tool_ok AUTH1 create_token '{"name":"mcp-smoke","expires_in_days":1}'
NEW_TOKEN=$(printf '%s' "$LAST" | jq -r '.token // empty')
TOKEN_ID=$(printf '%s' "$LAST" | jq -r '.id // empty')
# The secret must never reach the output, including a FAIL dump.
LAST=$(printf '%s' "$LAST" | jq -c 'if has("token") then .token = "<redacted>" else . end')
[ -n "$NEW_TOKEN" ] && [ -n "$TOKEN_ID" ] || fail "create_token returned no token or id"
case "$NEW_TOKEN" in arneb_*) ;; *) fail "new token does not start with arneb_" ;; esac
tool_ok AUTH1 list_tokens
check "any(.tokens[]; .id == \"$TOKEN_ID\" and .name == \"mcp-smoke\")" "list_tokens lacks the new token"
check 'all(.tokens[]; has("token") | not)' "list_tokens exposes a secret"
tool_ok NEW whoami
check ".user == \"$USER1\"" "whoami with the new token is not $USER1"
check '.via == "token"' "whoami with the new token is not via token"
tool_ok AUTH1 revoke_token "$(jq -nc --arg id "$TOKEN_ID" '{id:$id}')"
check ".revoked == \"$TOKEN_ID\"" "revoke_token did not answer revoked"
post NEW '{"jsonrpc":"2.0","id":97,"method":"tools/call","params":{"name":"whoami","arguments":{}}}'
[ "$CODE" = 401 ] || fail "revoked token got HTTP $CODE, want 401"
LAST=""
NEW_TOKEN=""
echo "  token id=$TOKEN_ID created, used, revoked"
pass
