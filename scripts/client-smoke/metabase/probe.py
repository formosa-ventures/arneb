"""Drive a fresh Metabase against a PostgreSQL-protocol database and report
what works: setup, add the database, wait for the sync, report the synced
metadata, run the query-builder and native questions, and run an x-ray.

Usage: probe.py METABASE_URL HOST PORT [DB_NAME]
                [--expect-tables N] [--expect-fields N]
                [--metrics URL] [--json OUT] [--jdbc-options OPTS]

  METABASE_URL  e.g. http://127.0.0.1:13300 (compose.yml)
  HOST PORT     the database as Metabase reaches it, e.g.
                host.docker.internal 15432 (Arneb on the host) or pgcap 5432
                (the capture PostgreSQL, from inside the compose network)
  DB_NAME       database to connect to (default arneb); user smoke, no
                password, SSL off
  --expect-*    fail unless the sync found exactly this many tables/fields
  --metrics     Arneb's Prometheus endpoint (http://127.0.0.1:16432/metrics
                on the client-smoke cluster): fail if
                arneb_pg_catalog_fallbacks_total moves during the probe
  --json        write every question's columns and rows to OUT, for
                comparing the data with DuckDB
  --jdbc-options
                Metabase's "Additional JDBC connection string options" for
                the database, e.g. binaryTransfer=false

Prints one OK/FAIL line per check, FIELD lines (table, column,
database_type, base_type) for diffing two runs, and finally
  PROBE ok=<n> fail=<m> tables=<t> fields=<f>
Exits non-zero when fail > 0 or an --expect-* value differs.

Standard library only. On first use it sets Metabase up with the admin
probe@example.com and a random password, kept in .probe-password next to
this script (git-ignored), so later runs against the same Metabase log in.
Each run deletes and re-adds its Metabase database entry, so every run
syncs from scratch.
"""
import argparse
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.request

ADMIN = "probe@example.com"
PASSWORD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".probe-password")


class Metabase:
    def __init__(self, url):
        self.url = url.rstrip("/")
        self.session = None

    def call(self, method, path, body=None, timeout=300):
        req = urllib.request.Request(self.url + path, method=method)
        req.add_header("Content-Type", "application/json")
        if self.session:
            req.add_header("X-Metabase-Session", self.session)
        data = json.dumps(body).encode() if body is not None else None
        try:
            with urllib.request.urlopen(req, data, timeout=timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, raw.decode(errors="replace")

    def wait_healthy(self):
        for _ in range(180):
            try:
                s, b = self.call("GET", "/api/health", timeout=5)
                if s == 200 and isinstance(b, dict) and b.get("status") == "ok":
                    return
            except OSError:
                pass
            time.sleep(2)
        sys.exit(f"{self.url} never became healthy")

    def login(self):
        _, props = self.call("GET", "/api/session/properties")
        token = props.get("setup-token") if isinstance(props, dict) else None
        if token and not props.get("has-user-setup"):
            # Metabase's default policy wants a digit; token_urlsafe may lack one.
            password = f"Mb-{secrets.token_urlsafe(12)}-{secrets.randbelow(90) + 10}"
            s, b = self.call("POST", "/api/setup", {
                "token": token,
                "user": {"first_name": "Probe", "last_name": "Admin", "email": ADMIN,
                         "password": password, "site_name": "arneb-probe"},
                "prefs": {"site_name": "arneb-probe", "allow_tracking": False},
            })
            if s != 200:
                sys.exit(f"setup failed {s}: {str(b).replace(password, '<password>')}")
            fd = os.open(PASSWORD_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(password)
            self.session = b["id"]
            return
        try:
            with open(PASSWORD_FILE) as f:
                password = f.read().strip()
        except FileNotFoundError:
            sys.exit(f"Metabase is already set up but {PASSWORD_FILE} is missing; "
                     "recreate it with `docker compose -p arneb-mb down -v` and `up -d`")
        s, b = self.call("POST", "/api/session", {"username": ADMIN, "password": password})
        if s != 200:
            sys.exit(f"login failed {s}: {b}")
        self.session = b["id"]


class Probe:
    def __init__(self, mb):
        self.mb = mb
        self.ok = 0
        self.fail = 0
        self.results = {}

    def check(self, passed, label, detail=""):
        if passed:
            self.ok += 1
            print(f"OK   {label} {detail}".rstrip())
        else:
            self.fail += 1
            print(f"FAIL {label} {detail}".rstrip())

    def question(self, label, query):
        t0 = time.time()
        s, b = self.mb.call("POST", "/api/dataset", query)
        ms = int((time.time() - t0) * 1000)
        if s in (200, 202) and isinstance(b, dict) and b.get("status") == "completed":
            rows = b["data"]["rows"]
            cols = [c["name"] for c in b["data"]["cols"]]
            self.results[label] = {"cols": cols, "rows": rows}
            self.check(True, label, f"rows={len(rows)} first={rows[:3]} {ms}ms")
            return
        err = b.get("error") if isinstance(b, dict) else b
        self.check(False, label, f"status={s} error={str(err)[:600]}")
        native = b.get("native_form") if isinstance(b, dict) else None
        if isinstance(native, dict) and native.get("query"):
            print(f"     sql={native['query'][:800]}")


def fallbacks(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        text = r.read().decode()
    return sum(float(line.rsplit(" ", 1)[1]) for line in text.splitlines()
               if line.startswith("arneb_pg_catalog_fallbacks_total"))


def add_database(probe, name, host, port, dbname, jdbc_options=None):
    mb = probe.mb
    _, dbs = mb.call("GET", "/api/database")
    for d in dbs.get("data", []) if isinstance(dbs, dict) else []:
        if d["name"] == name:
            mb.call("DELETE", f"/api/database/{d['id']}")
    s, b = mb.call("POST", "/api/database", {
        "engine": "postgres", "name": name,
        "details": {"host": host, "port": port, "dbname": dbname, "user": "smoke",
                    "password": "", "ssl": False, "schema-filters-type": "all",
                    **({"additional-options": jdbc_options} if jdbc_options else {})},
        "is_full_sync": True,
    })
    probe.check(s == 200, "add database", "" if s == 200 else json.dumps(b)[:2000])
    if s != 200:
        return None
    db = b["id"]
    status = None
    for _ in range(150):
        _, d = mb.call("GET", f"/api/database/{db}")
        status = d.get("initial_sync_status") if isinstance(d, dict) else None
        if status in ("complete", "aborted"):
            break
        time.sleep(2)
    probe.check(status == "complete", "sync", f"initial_sync_status={status}")
    return db


def report_metadata(probe, db):
    _, md = probe.mb.call("GET", f"/api/database/{db}/metadata?include_hidden=true")
    tables = {t["name"]: t for t in (md.get("tables", []) if isinstance(md, dict) else [])}
    for name in sorted(tables):
        t = tables[name]
        fields = sorted(t.get("fields", []), key=lambda f: f.get("position", 0))
        print(f"TABLE {t.get('schema')}.{name} fields={len(fields)}")
        for f in fields:
            print(f"FIELD {t.get('schema')}.{name}.{f['name']} "
                  f"{f.get('database_type')} {f.get('base_type')}")
    return tables


def questions(db, tabs):
    def fid(table, col):
        return next(f["id"] for f in tabs[table]["fields"] if f["name"] == col)

    def field(table, col, opts=None):
        return ["field", fid(table, col), opts]

    def mbql(table, **query):
        return {"database": db, "type": "query",
                "query": {"source-table": tabs[table]["id"], **query}}

    def native(sql):
        return {"database": db, "type": "native", "native": {"query": sql}}

    def o(col, opts=None):
        return field("orders", col, opts)

    def by_date(unit):
        return o("o_orderdate", {"temporal-unit": unit})

    count = [["count"]]
    n_name = field("nation", "n_name")
    yield "native count", native("SELECT count(*) FROM lineitem")
    yield "native date", native(
        "SELECT o_orderdate, o_totalprice FROM orders ORDER BY o_orderkey LIMIT 3")
    yield "table preview orders", mbql("orders", limit=2000)
    yield "count orders", mbql("orders", aggregation=count)
    yield "sum by status", mbql("orders", aggregation=[["sum", o("o_totalprice")]],
                                breakout=[o("o_orderstatus")])
    yield "count by month", mbql("orders", aggregation=count, breakout=[by_date("month")], limit=5)
    yield "count by year", mbql("orders", aggregation=count, breakout=[by_date("year")])
    yield "count by quarter-of-year", mbql("orders", aggregation=count,
                                           breakout=[by_date("quarter-of-year")])
    yield "count by day-of-week", mbql("orders", aggregation=count,
                                       breakout=[by_date("day-of-week")])
    yield "count by week", mbql("orders", aggregation=count, breakout=[by_date("week")], limit=3)
    yield "filter date range", mbql("orders", aggregation=count, filter=[
        "between", o("o_orderdate"), "1995-01-01", "1995-12-31"])
    yield "filter relative", mbql("orders", aggregation=count, filter=[
        "time-interval", o("o_orderdate"), -30, "year"])
    yield "filter contains", mbql("customer", aggregation=count, filter=[
        "contains", field("customer", "c_name"), "0001", {"case-sensitive": False}])
    yield "filter starts-with", mbql("nation", filter=["starts-with", n_name, "A"])
    yield "distinct count", mbql("orders", aggregation=[["distinct", o("o_custkey")]])
    yield "avg min max stddev", mbql("orders", aggregation=[
        ["avg", o("o_totalprice")], ["min", o("o_totalprice")],
        ["max", o("o_totalprice")], ["stddev", o("o_totalprice")]])
    yield "median", mbql("orders", aggregation=[["median", o("o_totalprice")]])
    yield "order by aggregate desc limit", mbql(
        "orders", aggregation=count, breakout=[o("o_custkey")],
        **{"order-by": [["desc", ["aggregation", 0]]]}, limit=5)
    yield "binning price", mbql("orders", aggregation=count, breakout=[
        o("o_totalprice", {"binning": {"strategy": "num-bins", "num-bins": 10}})])
    yield "expression column", mbql(
        "orders", expressions={"double": ["*", o("o_totalprice"), 2]},
        fields=[o("o_orderkey"), ["expression", "double"]], limit=3)
    yield "cumulative count", mbql("orders", aggregation=[["cum-count"]],
                                   breakout=[by_date("year")])
    yield "explicit join", mbql("customer", joins=[{
        "source-table": tabs["nation"]["id"], "alias": "N", "fields": "all",
        "condition": ["=", field("customer", "c_nationkey"),
                      ["field", fid("nation", "n_nationkey"), {"join-alias": "N"}]]}],
        aggregation=count,
        breakout=[["field", fid("nation", "n_name"), {"join-alias": "N"}]])
    yield "nested question", {"database": db, "type": "query", "query": {
        "source-query": {"source-table": tabs["orders"]["id"], "aggregation": count,
                         "breakout": [o("o_custkey")]},
        "aggregation": [["avg", ["field", "count", {"base-type": "type/Integer"}]]]}}
    yield "case expression", mbql(
        "orders", expressions={"big": ["case", [[[">", o("o_totalprice"), 100000], "big"]],
                                       {"default": "small"}]},
        aggregation=count, breakout=[["expression", "big"]])
    yield "string functions", mbql(
        "nation", expressions={"u": ["upper", n_name], "l": ["length", n_name],
                               "s": ["substring", n_name, 1, 3]},
        fields=[["expression", "u"], ["expression", "l"], ["expression", "s"]], limit=2)
    yield "datetime-diff", mbql(
        "orders", expressions={"d": ["datetime-diff", o("o_orderdate"), "2000-01-01", "day"]},
        fields=[["expression", "d"]], limit=2)
    yield "get-year", mbql("orders", expressions={"y": ["get-year", o("o_orderdate")]},
                           fields=[["expression", "y"]], limit=2)
    yield "native parameter", {"database": db, "type": "native", "native": {
        "query": "SELECT count(*) FROM nation WHERE n_regionkey = {{r}}",
        "template-tags": {"r": {"id": "r1", "name": "r", "display-name": "r", "type": "number"}}},
        "parameters": [{"type": "number/=", "value": [1],
                        "target": ["variable", ["template-tag", "r"]]}]}


def main():
    ap = argparse.ArgumentParser(
        usage="probe.py METABASE_URL HOST PORT [DB_NAME] [options]",
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("metabase_url")
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument("db_name", nargs="?", default="arneb")
    ap.add_argument("--expect-tables", type=int)
    ap.add_argument("--expect-fields", type=int)
    ap.add_argument("--metrics")
    ap.add_argument("--json")
    ap.add_argument("--jdbc-options")
    args = ap.parse_args()

    mb = Metabase(args.metabase_url)
    mb.wait_healthy()
    mb.login()
    probe = Probe(mb)
    before = fallbacks(args.metrics) if args.metrics else None

    db = add_database(probe, f"{args.db_name}@{args.host}:{args.port}",
                      args.host, args.port, args.db_name, args.jdbc_options)
    tabs = report_metadata(probe, db) if db is not None else {}
    n_fields = sum(len(t.get("fields", [])) for t in tabs.values())
    needed = {"orders", "customer", "nation"}
    if db is not None and needed <= tabs.keys():
        for f in tabs["nation"]["fields"]:
            s, b = mb.call("GET", f"/api/field/{f['id']}/values")
            n = len(b.get("values", [])) if isinstance(b, dict) else 0
            probe.check(s == 200 and n > 0, f"field values nation.{f['name']}", f"n={n}")
        for label, query in questions(db, tabs):
            probe.question(label, query)
        s, b = mb.call("GET", f"/api/automagic-dashboards/table/{tabs['orders']['id']}")
        cards = [dc["card"] for dc in (b.get("dashcards", []) if isinstance(b, dict) else [])
                 if isinstance(dc.get("card"), dict) and dc["card"].get("dataset_query")]
        probe.check(s == 200 and cards, "x-ray orders", f"status={s} cards={len(cards)}")
        for card in cards:
            probe.question(f"x-ray card {card.get('name')!r}", card["dataset_query"])
    else:
        probe.check(False, "questions", f"tables {sorted(needed - tabs.keys())} missing")

    if args.metrics:
        delta = fallbacks(args.metrics) - before
        probe.check(delta == 0, "pg_catalog fallbacks", f"delta={delta:g}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(probe.results, f, indent=1, default=str)

    print(f"PROBE ok={probe.ok} fail={probe.fail} tables={len(tabs)} fields={n_fields}")
    bad = probe.fail > 0
    for name, want, got in (("tables", args.expect_tables, len(tabs)),
                            ("fields", args.expect_fields, n_fields)):
        if want is not None and want != got:
            print(f"expected {want} {name}, got {got}")
            bad = True
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
