#!/usr/bin/env python3
"""Generate TPC-H Parquet files using Docker Trino's built-in tpch connector.

Usage:
    python3 scripts/generate_parquet.py [--scale-factor sf1] [--output-dir data/sf1]

Requires: Docker, requests, pyarrow
"""

import argparse
import json
import os
import subprocess
import sys
import time

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow is required. Install with: pip install pyarrow")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

TRINO_IMAGE = "trinodb/trino:latest"
CONTAINER_NAME = "tpch-data-gen"
TRINO_PORT = 18080  # Use non-standard port to avoid conflicts

TABLES = [
    "lineitem", "orders", "customer", "part", "partsupp",
    "supplier", "nation", "region"
]


def start_trino():
    """Start Docker Trino container if not already running."""
    result = subprocess.run(
        ["docker", "ps", "--filter", f"name={CONTAINER_NAME}", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    if CONTAINER_NAME in result.stdout:
        print(f"Trino container '{CONTAINER_NAME}' already running")
        return

    # Remove stopped container if exists
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)

    print(f"Starting Trino container on port {TRINO_PORT}...")
    subprocess.run([
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", f"{TRINO_PORT}:8080",
        TRINO_IMAGE
    ], check=True)

    # Wait for Trino to be ready
    print("Waiting for Trino to start...", end="", flush=True)
    for i in range(60):
        try:
            r = requests.get(f"http://localhost:{TRINO_PORT}/v1/info", timeout=2)
            if r.status_code == 200 and r.json().get("starting") is False:
                print(" ready!")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
    print("\nERROR: Trino did not start in time")
    sys.exit(1)


def query_trino(sql):
    """Execute a query via Trino REST API and return rows + column names."""
    url = f"http://localhost:{TRINO_PORT}/v1/statement"
    headers = {"X-Trino-User": "benchmark", "X-Trino-Catalog": "tpch", "X-Trino-Schema": "sf1"}

    r = requests.post(url, data=sql, headers=headers)
    result = r.json()

    columns = []
    all_rows = []

    while True:
        if "columns" in result and not columns:
            columns = [c["name"] for c in result["columns"]]
        if "data" in result:
            all_rows.extend(result["data"])
        if "nextUri" not in result:
            break
        time.sleep(0.1)
        r = requests.get(result["nextUri"], headers=headers)
        result = r.json()

    if "error" in result:
        raise RuntimeError(f"Trino query failed: {result['error']['message']}")

    return columns, all_rows


def get_column_types(table, sf):
    """Get column types from Trino for a table."""
    sql = f"DESCRIBE tpch.{sf}.{table}"
    columns, rows = query_trino(sql)
    # rows: [column_name, type, extra, comment]
    col_types = []
    for row in rows:
        col_name = row[0]
        col_type = row[1]
        col_types.append((col_name, col_type))
    return col_types


def trino_type_to_arrow(trino_type):
    """Convert Trino type string to PyArrow type."""
    t = trino_type.lower()
    if t == "bigint":
        return pa.int64()
    if t == "integer":
        return pa.int32()
    if t == "double":
        return pa.float64()
    if t.startswith("varchar") or t == "char(1)":
        return pa.string()
    if t.startswith("char"):
        return pa.string()
    if t == "date":
        return pa.date32()
    if t.startswith("decimal"):
        # Parse decimal(p,s)
        inner = t[len("decimal("):-1]
        parts = inner.split(",")
        p, s = int(parts[0].strip()), int(parts[1].strip())
        return pa.decimal128(p, s)
    return pa.string()  # fallback


def export_table(table, sf, output_dir):
    """Export a single TPC-H table as Parquet."""
    print(f"  Exporting {table}...", end=" ", flush=True)

    # Get schema
    col_types = get_column_types(table, sf)

    # Query all data
    sql = f"SELECT * FROM tpch.{sf}.{table}"
    col_names, rows = query_trino(sql)

    # Build Arrow table
    arrow_fields = []
    for col_name, trino_type in col_types:
        arrow_fields.append(pa.field(col_name, trino_type_to_arrow(trino_type)))

    schema = pa.schema(arrow_fields)

    # Convert rows to columnar
    if not rows:
        table_data = pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
    else:
        columns = {}
        for i, field in enumerate(schema):
            col_values = [row[i] for row in rows]
            if field.type == pa.date32():
                # Convert date strings to date objects
                import datetime
                col_values = [
                    datetime.date.fromisoformat(v) if v else None
                    for v in col_values
                ]
            columns[field.name] = pa.array(col_values, type=field.type)
        table_data = pa.table(columns, schema=schema)

    # Write Parquet
    output_path = os.path.join(output_dir, f"{table}.parquet")
    pq.write_table(table_data, output_path, compression="snappy")
    print(f"{len(rows)} rows -> {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H Parquet data via Docker Trino")
    parser.add_argument("--scale-factor", default="sf1", help="TPC-H scale factor (default: sf1)")
    parser.add_argument("--output-dir", default="benchmarks/tpch/data/sf1", help="Output directory")
    parser.add_argument("--keep-container", action="store_true", help="Keep Trino container running after export")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    start_trino()

    print(f"\nExporting TPC-H {args.scale_factor} tables to {args.output_dir}/")
    for table in TABLES:
        export_table(table, args.scale_factor, args.output_dir)

    if not args.keep_container:
        print(f"\nStopping Trino container...")
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)

    print(f"\nDone! Parquet files in {args.output_dir}/")
    print(f"Tables: {', '.join(TABLES)}")


if __name__ == "__main__":
    main()
