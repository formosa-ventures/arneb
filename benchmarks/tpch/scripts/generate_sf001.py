#!/usr/bin/env python3
"""
Generate TPC-H SF0.01 Parquet files from raw .tbl dbgen output with
TPC-H-correct column types (notably Date32 for date columns).

Input:  benchmarks/tpch/data/raw/<table>.tbl   (pipe-delimited, dbgen format)
Output: benchmarks/tpch/data/sf001/<table>.parquet

Types follow the convention used by Trino's built-in tpch connector so that
the local Parquet files are schema-compatible with the Hive-backed TPC-H
tables produced by docker/tpch-seed/seed.sh. Integer keys are int64, numeric
money/quantity fields are float64, and dates are date32[day].

Usage:
  python3 benchmarks/tpch/scripts/generate_sf001.py \\
      [--raw-dir benchmarks/tpch/data/raw] \\
      [--out-dir benchmarks/tpch/data/sf001]
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

INT = pa.int64()
F64 = pa.float64()
STR = pa.string()
DATE = pa.date32()

# (column_name, pyarrow_type) tuples in dbgen column order.
TPCH_SCHEMAS: dict[str, list[tuple[str, pa.DataType]]] = {
    "region": [
        ("r_regionkey", INT),
        ("r_name", STR),
        ("r_comment", STR),
    ],
    "nation": [
        ("n_nationkey", INT),
        ("n_name", STR),
        ("n_regionkey", INT),
        ("n_comment", STR),
    ],
    "supplier": [
        ("s_suppkey", INT),
        ("s_name", STR),
        ("s_address", STR),
        ("s_nationkey", INT),
        ("s_phone", STR),
        ("s_acctbal", F64),
        ("s_comment", STR),
    ],
    "customer": [
        ("c_custkey", INT),
        ("c_name", STR),
        ("c_address", STR),
        ("c_nationkey", INT),
        ("c_phone", STR),
        ("c_acctbal", F64),
        ("c_mktsegment", STR),
        ("c_comment", STR),
    ],
    "part": [
        ("p_partkey", INT),
        ("p_name", STR),
        ("p_mfgr", STR),
        ("p_brand", STR),
        ("p_type", STR),
        ("p_size", INT),
        ("p_container", STR),
        ("p_retailprice", F64),
        ("p_comment", STR),
    ],
    "partsupp": [
        ("ps_partkey", INT),
        ("ps_suppkey", INT),
        ("ps_availqty", INT),
        ("ps_supplycost", F64),
        ("ps_comment", STR),
    ],
    "orders": [
        ("o_orderkey", INT),
        ("o_custkey", INT),
        ("o_orderstatus", STR),
        ("o_totalprice", F64),
        ("o_orderdate", DATE),
        ("o_orderpriority", STR),
        ("o_clerk", STR),
        ("o_shippriority", INT),
        ("o_comment", STR),
    ],
    "lineitem": [
        ("l_orderkey", INT),
        ("l_partkey", INT),
        ("l_suppkey", INT),
        ("l_linenumber", INT),
        ("l_quantity", F64),
        ("l_extendedprice", F64),
        ("l_discount", F64),
        ("l_tax", F64),
        ("l_returnflag", STR),
        ("l_linestatus", STR),
        ("l_shipdate", DATE),
        ("l_commitdate", DATE),
        ("l_receiptdate", DATE),
        ("l_shipinstruct", STR),
        ("l_shipmode", STR),
        ("l_comment", STR),
    ],
}


def parse_value(raw: str, dtype: pa.DataType):
    if dtype == INT:
        return int(raw)
    if dtype == F64:
        return float(raw)
    if dtype == DATE:
        y, m, d = raw.split("-")
        return date(int(y), int(m), int(d))
    return raw


def load_tbl(tbl_path: Path, schema_def: list[tuple[str, pa.DataType]]) -> pa.Table:
    columns: list[list] = [[] for _ in schema_def]
    with tbl_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            # dbgen appends a trailing '|' which csv.reader turns into an empty field.
            if len(row) == len(schema_def) + 1 and row[-1] == "":
                row = row[:-1]
            if len(row) != len(schema_def):
                raise ValueError(
                    f"{tbl_path.name}: expected {len(schema_def)} columns, got {len(row)}: {row!r}"
                )
            for i, (_, dtype) in enumerate(schema_def):
                columns[i].append(parse_value(row[i], dtype))

    arrays = [pa.array(col, type=dtype) for col, (_, dtype) in zip(columns, schema_def)]
    schema = pa.schema([(name, dtype) for name, dtype in schema_def])
    return pa.Table.from_arrays(arrays, schema=schema)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("benchmarks/tpch/data/raw"),
        help="Directory containing <table>.tbl dbgen files",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("benchmarks/tpch/data/sf001"),
        help="Output directory for <table>.parquet files",
    )
    args = parser.parse_args()

    if not args.raw_dir.is_dir():
        print(f"raw dir not found: {args.raw_dir}", file=sys.stderr)
        return 1
    args.out_dir.mkdir(parents=True, exist_ok=True)

    for table, schema_def in TPCH_SCHEMAS.items():
        tbl_path = args.raw_dir / f"{table}.tbl"
        out_path = args.out_dir / f"{table}.parquet"
        if not tbl_path.exists():
            print(f"skip {table}: {tbl_path} not found", file=sys.stderr)
            continue
        table_data = load_tbl(tbl_path, schema_def)
        pq.write_table(table_data, out_path, compression="snappy")
        print(f"wrote {out_path}  rows={table_data.num_rows}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
