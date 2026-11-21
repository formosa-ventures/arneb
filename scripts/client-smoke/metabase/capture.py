"""Turn a PostgreSQL log_statement=all log into a corpus file, and optionally
replay that corpus on a server.

Usage: capture.py PGLOG OUT_SQL [--replay HOST:PORT] [--all-sessions]

  PGLOG       the pgcap log (`docker compose -p arneb-mb logs --no-log-prefix
              pgcap > pgcap.log`); its line prefix is @@%m|%c|%v@@ (compose.yml)
  OUT_SQL     the corpus: one statement per entry, entries separated by a
              `;;;;` line (no trailing separator), the format of
              docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-captured.sql
  --replay    run every statement over the simple-query protocol (user smoke,
              database arneb) and print an OK/FAIL line per statement, then
              `TOTAL ok=<n> fail=<m>`; exits non-zero when fail > 0
  --all-sessions
              keep every session; by default only sessions that set an
              application_name (Metabase's connections) are kept, which drops
              the image's init scripts and the seeding

Bind parameters (the `DETAIL:  Parameters:` lines of the extended protocol)
are inlined, the application_name and Metabase's queryHash comments are
masked so two captures diff cleanly, and repeated statements are kept once,
in first-seen order.

--replay needs psycopg2 (scripts/client-smoke/requirements.txt); the capture
itself is standard library only.
"""
import argparse
import re
import sys

PREFIX = re.compile(r"^@@[^|]*\|(?P<session>[^|]*)\|[^@]*@@ (?P<rest>.*)$")
STATEMENT = re.compile(r"LOG:  (?:statement|execute [^:]*): (.*)", re.S)
PARAMETERS = re.compile(r"DETAIL:  Parameters: (.*)", re.S)
PARAMETER = re.compile(r"\$(\d+) = ('(?:[^']|'')*'|NULL)")


def entries(lines):
    """Yield (session, text) per log entry; continuation lines are joined."""
    session = text = None
    for line in lines:
        m = PREFIX.match(line.rstrip("\n"))
        if m:
            if text is not None:
                yield session, text
            session, text = m.group("session"), m.group("rest")
        elif text is not None:
            text += "\n" + line.rstrip("\n")
    if text is not None:
        yield session, text


def statements(lines):
    """[(session, sql, {n: literal} | None)] in log order."""
    out = []
    for session, text in entries(lines):
        m = STATEMENT.match(text)
        if m:
            out.append([session, m.group(1).strip(), None])
            continue
        m = PARAMETERS.match(text)
        if m and out and out[-1][0] == session:
            out[-1][2] = dict(PARAMETER.findall(m.group(1)))
    return out


def inline(sql, params):
    # Highest number first, so $1 does not eat the start of $10.
    for n in sorted(params, key=int, reverse=True):
        sql = re.sub(r"\$" + n + r"(?!\d)", lambda _m, v=params[n]: v, sql)
    return sql


def mask(sql):
    sql = re.sub(r"(?i)(application_name\s*=\s*)'(?:[^']|'')*'", r"\1'Metabase'", sql)
    return re.sub(r"queryHash: [0-9a-f]+", "queryHash: <hash>", sql)


def corpus(lines, all_sessions=False):
    stmts = statements(lines)
    keep = None
    if not all_sessions:
        keep = {s for s, sql, _ in stmts if re.match(r"(?i)set\s+application_name\b", sql)}
    seen, out = set(), []
    for session, sql, params in stmts:
        if not sql or (keep is not None and session not in keep):
            continue
        sql = mask(inline(sql, params) if params else sql)
        if sql not in seen:
            seen.add(sql)
            out.append(sql)
    return out


def replay(stmts, host, port):
    import psycopg2

    def connect():
        c = psycopg2.connect(host=host, port=port, user="smoke", dbname="arneb")
        c.autocommit = True
        return c

    conn = connect()
    ok = fail = 0
    for i, sql in enumerate(stmts):
        flat = " ".join(sql.split())
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.rowcount
            ok += 1
            print(f"[{i}] OK   rows={rows} :: {flat[:160]}")
        except Exception as e:  # report every statement, whatever it raises
            fail += 1
            first = (str(e).strip().splitlines() or [type(e).__name__])[0]
            print(f"[{i}] FAIL {first[:300]} :: {flat[:400]}")
            if conn.closed:
                conn = connect()
    print(f"TOTAL ok={ok} fail={fail}")
    return fail == 0


def main():
    ap = argparse.ArgumentParser(
        usage="capture.py PGLOG OUT_SQL [--replay HOST:PORT] [--all-sessions]",
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pglog")
    ap.add_argument("out_sql")
    ap.add_argument("--replay", metavar="HOST:PORT")
    ap.add_argument("--all-sessions", action="store_true")
    args = ap.parse_args()

    with open(args.pglog, errors="replace") as f:
        stmts = corpus(f, args.all_sessions)
    with open(args.out_sql, "w") as f:
        f.write("\n;;;;\n".join(stmts) + "\n")
    print(f"wrote {len(stmts)} statements to {args.out_sql}", file=sys.stderr)
    if args.replay:
        host, _, port = args.replay.rpartition(":")
        sys.exit(0 if replay(stmts, host or "127.0.0.1", int(port)) else 1)


if __name__ == "__main__":
    main()
