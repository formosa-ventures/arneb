#!/usr/bin/env bash
# MCP OAuth smoke test (an OAuth access token on /mcp, [oauth] in arneb.toml).
#
# Runs the acceptance scenario of the mcp-oauth change against a live stack: a
# coordinator plus two workers with password auth, `admin_users = [alice]`, a
# hive catalog over the TPC-H tables, and the Keycloak dev realm of
# docker/keycloak/arneb-realm.json (docker compose --profile oauth up -d
# keycloak). The coordinator runs with
#   [oauth] issuer = "http://127.0.0.1:8180/realms/arneb"
#           resource = "http://127.0.0.1:<web port>/mcp"
#           user_claim = "preferred_username"
# and the realm's audience mapper must name that same resource URL. The Arneb
# user alice exists (config or API); carol is created here through the API;
# bob has no Arneb user.
#
# Needs curl and jq. Prints `PASS <step>` per check and exits non-zero at the
# first `FAIL <step>`. Passwords, API tokens and JWTs are never printed, and
# never put on a command line (where `ps` shows them): curl gets credentials,
# form fields and bodies as a config on stdin (`curl -K -`), and jq reads
# secrets from the environment.
#
# Environment:
#   ARNEB_URL             Web port base URL, e.g. http://127.0.0.1:7432 (required)
#   ARNEB_ADMIN_USER / ARNEB_ADMIN_PASSWORD   an Arneb user in admin_users; the
#                         user name must equal the Keycloak user alice (required)
#   KEYCLOAK_ISSUER       default http://127.0.0.1:8180/realms/arneb
#   KC_PASSWORD_ALICE / KC_PASSWORD_BOB / KC_PASSWORD_CAROL   the realm users'
#                         passwords (default <name>-dev-only-pw, the dev realm's)
#   OAUTH_CLIENT          client with the audience mapper (default arneb-smoke)
#   OAUTH_OTHER_CLIENT    client without it (default arneb-other)
#   ARNEB_CATALOG / ARNEB_SCHEMA   where the TPC-H tables are (default datalake / tpch)
#   ARNEB_EXPECTED_LINEITEM   count(*) of lineitem (default 59986052, SF10)
#   ARNEB_COORD_LOG       coordinator log file; when set, step 9 scans it
#   ARNEB_AUDIT_FILE      audit.jsonl; when set, step 9 scans it
#
# Usage: scripts/mcp-oauth-smoke.sh

set -euo pipefail

: "${ARNEB_URL:?set ARNEB_URL to the Web port, e.g. http://127.0.0.1:7432}"
: "${ARNEB_ADMIN_USER:?set ARNEB_ADMIN_USER to alice (a user in admin_users)}"
: "${ARNEB_ADMIN_PASSWORD:?set ARNEB_ADMIN_PASSWORD}"
ISSUER="${KEYCLOAK_ISSUER:-http://127.0.0.1:8180/realms/arneb}"
CLIENT="${OAUTH_CLIENT:-arneb-smoke}"
OTHER_CLIENT="${OAUTH_OTHER_CLIENT:-arneb-other}"
CAT="${ARNEB_CATALOG:-datalake}"
SCHEMA="${ARNEB_SCHEMA:-tpch}"
EXPECTED="${ARNEB_EXPECTED_LINEITEM:-59986052}"
PW_ALICE="${KC_PASSWORD_ALICE:-alice-dev-only-pw}"
PW_BOB="${KC_PASSWORD_BOB:-bob-dev-only-pw}"
PW_CAROL="${KC_PASSWORD_CAROL:-carol-dev-only-pw}"
BASE="${ARNEB_URL%/}"
API="$BASE/api/v2"
MCP="$BASE/mcp"
ADMIN="$ARNEB_ADMIN_USER"
[ "$ADMIN" = alice ] || { echo "FAIL 0 setup: ARNEB_ADMIN_USER must be alice (the Keycloak user)"; exit 1; }

STEP=""
LAST=""
CODE=""
HEADERS=""
RID=0
JWT_ALICE=""
JWT_BOB=""
JWT_CAROL=""
JWT_OTHER=""
APITOK=""
CAROL_ARNEB_PW=""
PROBE_PW=""
pass() { echo "PASS $STEP"; }
fail() {
    # stderr, so the message survives a command substitution (`X=$(kc_token ...)`).
    echo "FAIL $STEP: $*" >&2
    [ -n "$LAST" ] && echo "  last response (HTTP ${CODE:-?}): $(printf '%s' "$LAST" | redact | head -c 2000)" >&2
    exit 1
}
# redact: strip every secret this run knows from text it is about to print.
redact() {
    local s
    s=$(cat)
    for secret in "$ARNEB_ADMIN_PASSWORD" "$PW_ALICE" "$PW_BOB" "$PW_CAROL" "$JWT_ALICE" "$JWT_BOB" \
        "$JWT_CAROL" "$JWT_OTHER" "$APITOK" "$CAROL_ARNEB_PW" "$PROBE_PW"; do
        [ -n "$secret" ] && s="${s//"$secret"/<secret>}"
    done
    # Anything token-shaped, whatever this run knows: an Arneb API token or a JWT.
    printf '%s' "$s" | sed -E 's/arneb_[0-9a-f]{16}_[A-Za-z0-9_-]+/arneb_<id>_<redacted>/g; s/eyJ[A-Za-z0-9_.-]+/eyJ<redacted>/g'
}
check() { # $1 = jq filter that must be true on $LAST, $2 = message
    printf '%s' "$LAST" | jq -e "$1" >/dev/null 2>&1 || fail "$2"
}
expect_code() { [ "$CODE" = "$1" ] || fail "$2 returned HTTP $CODE, want $1"; }

# cfgq <text>: <text> as a double-quoted curl config value.
cfgq() {
    local v="${1//\\/\\\\}"
    v="${v//\"/\\\"}"
    printf '"%s"' "$v"
}

# bearer_of <ALICE|BOB|CAROL|OTHER|APITOK>: the token for that identity.
bearer_of() {
    case "$1" in
        ALICE) printf '%s' "$JWT_ALICE" ;;
        BOB) printf '%s' "$JWT_BOB" ;;
        CAROL) printf '%s' "$JWT_CAROL" ;;
        OTHER) printf '%s' "$JWT_OTHER" ;;
        APITOK) printf '%s' "$APITOK" ;;
        *) fail "unknown identity $1" ;;
    esac
}

# curl_cfg <ADMIN|NONE|ALICE|...> [body]: a curl config carrying the
# credentials (Basic for ADMIN, a bearer token otherwise) and the body, for
# `curl -K -`. printf is a shell builtin, so none of it reaches an argv.
curl_cfg() {
    case "$1" in
        ADMIN) printf 'user = %s\n' "$(cfgq "$ADMIN:$ARNEB_ADMIN_PASSWORD")" ;;
        NONE) ;;
        *) printf 'header = %s\n' "$(cfgq "Authorization: Bearer $(bearer_of "$1")")" ;;
    esac
    if [ $# -ge 2 ]; then
        printf 'data-raw = %s\n' "$(cfgq "$2")"
    fi
}

# req <identity> <method> <url> [json-body]: body to $LAST, status to $CODE,
# response headers to $HEADERS.
req() {
    local out
    if [ $# -ge 4 ]; then
        out=$(curl_cfg "$1" "$4" | curl -sS -K - -i -X "$2" "$3" \
            -H 'Content-Type: application/json' -w '\n%{http_code}') || fail "curl $2 $3"
    else
        out=$(curl_cfg "$1" | curl -sS -K - -i -X "$2" "$3" -w '\n%{http_code}') || fail "curl $2 $3"
    fi
    CODE="${out##*$'\n'}"
    out="${out%$'\n'*}"
    # -i output: possibly a `100 Continue` block, then headers, a blank line, the body.
    HEADERS="${out%%$'\r\n\r\n'*}"
    LAST="${out#*$'\r\n\r\n'}"
}

# header_of <name>: the value of that response header from $HEADERS.
header_of() {
    printf '%s' "$HEADERS" | tr -d '\r' | grep -i "^$1:" | head -1 | cut -d: -f2- | sed 's/^ *//' || true
}

# mcp_post <identity> <json-rpc-body>: POST /mcp; body to $LAST, status to $CODE.
mcp_post() {
    local out
    out=$(curl_cfg "$1" "$2" | curl -sS -K - -i -X POST "$MCP" \
        -H 'Content-Type: application/json' \
        -H 'Accept: application/json, text/event-stream' \
        -H 'MCP-Protocol-Version: 2025-06-18' -w '\n%{http_code}') || fail "curl POST /mcp"
    CODE="${out##*$'\n'}"
    out="${out%$'\n'*}"
    HEADERS="${out%%$'\r\n\r\n'*}"
    LAST="${out#*$'\r\n\r\n'}"
}

# mcp_tool <identity> <name> [args-json]: tools/call, $LAST = result.
mcp_tool() {
    local targs="${3:-}"
    [ -n "$targs" ] || targs='{}'
    RID=$(( RID + 1 ))
    # The arguments may hold a password: jq gets them from the environment, not argv.
    mcp_post "$1" "$(TARGS="$targs" jq -nc --argjson id "$RID" --arg n "$2" \
        '{jsonrpc:"2.0", id:$id, method:"tools/call", params:{name:$n, arguments:(env.TARGS | fromjson)}}')"
    expect_code 200 "tools/call $2"
    check '.error == null' "tools/call $2 returned a JSON-RPC error"
    LAST=$(printf '%s' "$LAST" | jq -c '.result')
}

# mcp_query <identity> <sql> [max_rows]: submit_query, then poll until
# terminal; $LAST = the finished statement (structuredContent).
mcp_query() {
    local args id state deadline
    args=$(jq -nc --arg sql "$2" --argjson m "${3:-0}" \
        '{sql:$sql, wait_ms:5000} + (if $m > 0 then {max_rows:$m} else {} end)')
    mcp_tool "$1" submit_query "$args"
    check '.isError == false' "submit_query failed"
    LAST=$(printf '%s' "$LAST" | jq -c '.structuredContent')
    id=$(printf '%s' "$LAST" | jq -r '.id')
    deadline=$(( $(date +%s) + 600 ))
    while :; do
        state=$(printf '%s' "$LAST" | jq -r '.state')
        case "$state" in finished | failed | cancelled) return 0 ;; esac
        [ "$(date +%s)" -lt "$deadline" ] || fail "statement $id still $state after 600 s"
        mcp_tool "$1" get_query_status "$(jq -nc --arg id "$id" '{id:$id, wait_ms:5000}')"
        LAST=$(printf '%s' "$LAST" | jq -c '.structuredContent')
    done
}

# kc_token <user> <password> <client>: an access token from the realm's
# password grant. The form (password included) goes to curl on stdin.
kc_token() {
    local out
    out=$({
        printf 'data-urlencode = %s\n' "$(cfgq "grant_type=password")"
        printf 'data-urlencode = %s\n' "$(cfgq "client_id=$3")"
        printf 'data-urlencode = %s\n' "$(cfgq "username=$1")"
        printf 'data-urlencode = %s\n' "$(cfgq "password=$2")"
    } | curl -sS -K - -X POST "$ISSUER/protocol/openid-connect/token") || fail "token request for $1 on $3"
    printf '%s' "$out" | jq -er '.access_token' 2>/dev/null \
        || fail "no access token for $1 on $3: $(printf '%s' "$out" | jq -c '{error, error_description}' 2>/dev/null)"
}

# claims <jwt>: the payload of a JWT as JSON (no verification; for reporting).
claims() {
    local p
    p=$(printf '%s' "$1" | cut -d. -f2 | tr '_-' '/+')
    while [ $(( ${#p} % 4 )) -ne 0 ]; do p="$p="; done
    printf '%s' "$p" | base64 -d 2>/dev/null
}

STEP="0 metadata document and the /mcp challenge"
for path in /.well-known/oauth-protected-resource /.well-known/oauth-protected-resource/mcp; do
    req NONE GET "$BASE$path"
    expect_code 200 "GET $path"
    check ".resource == \"$BASE/mcp\" and .authorization_servers == [\"$ISSUER\"] and (.bearer_methods_supported | index(\"header\"))" \
        "metadata at $path is wrong"
done
mcp_post NONE '{"jsonrpc":"2.0","id":0,"method":"tools/list","params":{}}'
expect_code 401 "unauthenticated POST /mcp"
HEADERS_VAL=$(header_of www-authenticate)
case "$HEADERS_VAL" in
    Bearer*resource_metadata=\"$BASE/.well-known/oauth-protected-resource*) ;;
    *) fail "WWW-Authenticate has no resource_metadata for the well-known route: $HEADERS_VAL" ;;
esac
echo "  challenge: $HEADERS_VAL"
req ADMIN GET "$API/whoami"
expect_code 200 "admin whoami"
check '.privileges.manage == true' "$ADMIN has no manage privilege (is $ADMIN in admin_users?)"
# carol: an Arneb user created through the API (leftover from an earlier run is replaced).
CAROL_ARNEB_PW=$(openssl rand -hex 16)
req ADMIN DELETE "$API/users/carol"
case "$CODE" in 200 | 404) ;; *) fail "removing a leftover carol returned HTTP $CODE" ;; esac
req ADMIN POST "$API/users" "$(CAROL_ARNEB_PW="$CAROL_ARNEB_PW" jq -nc '{name:"carol", password:env.CAROL_ARNEB_PW}')"
expect_code 200 "create carol"
req ADMIN GET "$API/users"
check 'all(.users[]; .name != "bob")' "bob exists as an Arneb user; this smoke needs bob to be unknown"
check 'any(.users[]; .name == "alice")' "alice is not an Arneb user"
pass

STEP="1 alice's token: initialize, tools/list, whoami"
JWT_ALICE=$(kc_token alice "$PW_ALICE" "$CLIENT")
echo "  token claims: $(claims "$JWT_ALICE" | jq -c '{iss, aud, azp, preferred_username, exp_in_s: (.exp - now | floor)}')"
mcp_post ALICE '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"oauth-smoke","version":"1"}}}'
expect_code 200 "initialize with alice's token"
check '.result.serverInfo.name != null' "initialize answered no serverInfo"
mcp_post ALICE '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
expect_code 200 "tools/list"
check '.result.tools | length == 44' "tools/list does not have exactly 44 tools"
mcp_tool ALICE whoami
check '.structuredContent.via == "oauth" and .structuredContent.user == "alice" and .structuredContent.privileges.manage == true' \
    "whoami is not alice via oauth with manage"
echo "  whoami: $(printf '%s' "$LAST" | jq -c '.structuredContent | {user, via, auth, role}')"
pass

STEP="2 discover and query over OAuth"
mcp_tool ALICE list_catalogs
check ".structuredContent.catalogs // .structuredContent.items | map(if type == \"object\" then .name else . end) | index(\"$CAT\") != null" "list_catalogs lacks $CAT"
mcp_tool ALICE list_tables "$(jq -nc --arg c "$CAT" --arg s "$SCHEMA" '{catalog:$c, schema:$s}')"
check "(.structuredContent.tables // .structuredContent.items) | map(if type == \"object\" then .name else . end) | index(\"lineitem\") != null" \
    "list_tables on $CAT.$SCHEMA lacks lineitem"
mcp_tool ALICE sample_table "$(jq -nc --arg c "$CAT" --arg s "$SCHEMA" '{catalog:$c, schema:$s, table:"nation", rows:5}')"
check '.isError == false' "sample_table failed"
check '(.structuredContent.first_page.rows // .structuredContent.rows) | length > 0' "sample_table returned no rows"
mcp_tool ALICE explain_query "$(jq -nc --arg q "SELECT count(*) FROM $CAT.$SCHEMA.lineitem" '{sql:$q}')"
check '.isError == false' "explain_query failed"
mcp_query ALICE "SELECT count(*) FROM $CAT.$SCHEMA.lineitem"
check '.state == "finished"' "count(*) statement did not finish"
GOT=$(printf '%s' "$LAST" | jq -r '.first_page.rows[0][0] | tostring')
[ "$GOT" = "$EXPECTED" ] || fail "count(*) of lineitem is $GOT, want $EXPECTED"
echo "  $CAT.$SCHEMA.lineitem: $GOT rows"
mcp_query ALICE "SELECT l_orderkey, l_comment FROM $CAT.$SCHEMA.lineitem"
check '.state == "finished" and .truncated == true and .truncated_by == "max_rows"' "a large SELECT is not truncated by max_rows"
echo "  large SELECT: truncated_by $(printf '%s' "$LAST" | jq -r '.truncated_by'), total_rows $(printf '%s' "$LAST" | jq -c '.total_rows')"
pass

STEP="3 bob (no Arneb user) is refused with 403"
JWT_BOB=$(kc_token bob "$PW_BOB" "$CLIENT")
mcp_post BOB '{"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}'
expect_code 403 "bob's token on /mcp"
check '.error.code == "forbidden"' "bob's refusal is not the forbidden envelope"
pass

STEP="4 a token for another client (no audience) is refused with 401"
JWT_OTHER=$(kc_token alice "$PW_ALICE" "$OTHER_CLIENT")
echo "  token claims: $(claims "$JWT_OTHER" | jq -c '{iss, aud, azp, preferred_username}')"
mcp_post OTHER '{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}'
expect_code 401 "the no-audience token on /mcp"
case "$(header_of www-authenticate)" in *resource_metadata=*) ;; *) fail "the 401 carries no resource_metadata challenge" ;; esac
pass

STEP="5 alice's JWT is not accepted on /api/v2"
req ALICE GET "$API/whoami"
expect_code 401 "alice's JWT on GET /api/v2/whoami"
pass

STEP="6 an Arneb API token still works on /mcp"
req ADMIN POST "$API/tokens" '{"name":"oauth-smoke"}'
expect_code 201 "issue an API token"
APITOK=$(printf '%s' "$LAST" | jq -r '.result.token // .token')
[ -n "$APITOK" ] && [ "$APITOK" != null ] || fail "no token in the issue answer"
mcp_tool APITOK whoami
check '.structuredContent.via == "token" and .structuredContent.user == "alice"' "whoami with the API token is not alice via token"
pass

STEP="7 deleting carol revokes her still-valid JWT"
JWT_CAROL=$(kc_token carol "$PW_CAROL" "$CLIENT")
mcp_post CAROL '{"jsonrpc":"2.0","id":5,"method":"tools/list","params":{}}'
expect_code 200 "carol's token before the delete"
req ADMIN DELETE "$API/users/carol"
expect_code 200 "delete carol"
mcp_post CAROL '{"jsonrpc":"2.0","id":6,"method":"tools/list","params":{}}'
expect_code 403 "carol's token after the delete"
pass

STEP="8 a manage action over OAuth is audited with via oauth"
PROBE_PW=$(openssl rand -hex 16)
mcp_tool ALICE create_user "$(PROBE_PW="$PROBE_PW" jq -nc '{name:"oauth_probe", password:env.PROBE_PW}')"
check '.isError == false' "create_user over OAuth failed"
req ADMIN GET "$API/audit?limit=50"
expect_code 200 "GET /audit"
check 'any(.entries[]; .action == "user.create" and .target == "oauth_probe" and .user == "alice" and .via == "oauth" and .outcome == "ok")' \
    "no user.create audit entry by alice via oauth"
req ADMIN DELETE "$API/users/oauth_probe"
expect_code 200 "delete oauth_probe"
req ADMIN DELETE "$API/tokens/$(printf '%s' "$APITOK" | cut -d_ -f2)"
case "$CODE" in 200 | 204) ;; *) echo "  note: revoking the smoke API token returned HTTP $CODE" ;; esac
pass

STEP="9 no token or JWT in the coordinator log or the audit file"
SCANNED=0
for f in "${ARNEB_COORD_LOG:-}" "${ARNEB_AUDIT_FILE:-}"; do
    [ -n "$f" ] || continue
    [ -r "$f" ] || fail "cannot read $f"
    SCANNED=$(( SCANNED + 1 ))
    # grep without -q: an early exit would SIGPIPE and pipefail would flip the result.
    if grep -E 'eyJ[A-Za-z0-9_-]{10,}|arneb_[0-9a-f]{16}_[A-Za-z0-9_-]{20,}' "$f" >/dev/null; then
        fail "$f holds a JWT or an API token"
    fi
    for secret in "$JWT_ALICE" "$JWT_BOB" "$JWT_CAROL" "$JWT_OTHER" "$APITOK" "$PW_ALICE" "$PW_BOB" \
        "$PW_CAROL" "$ARNEB_ADMIN_PASSWORD" "$CAROL_ARNEB_PW" "$PROBE_PW"; do
        # An empty pattern would match every line.
        [ -n "$secret" ] || continue
        # The pattern comes from a pipe (-f), never from argv.
        if grep -F -f <(printf '%s\n' "$secret") "$f" >/dev/null; then fail "$f holds a secret this run used"; fi
    done
    echo "  $f: clean"
done
if [ "$SCANNED" -gt 0 ]; then
    pass
else
    echo "SKIP $STEP: set ARNEB_COORD_LOG or ARNEB_AUDIT_FILE to scan them"
fi
