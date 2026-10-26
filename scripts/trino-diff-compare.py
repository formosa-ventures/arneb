#!/usr/bin/env python3
"""Float-tolerant, positional CSV diff between an Arneb and a Trino result.

Usage: trino-diff-compare.py ARNEB.csv TRINO.csv TOL
Exit 0 when every cell matches; exit 1 and print up to 20 mismatches otherwise.
"""
import csv
import sys


def parse_float(s):
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def cells_equal(a, b, tol):
    fa, fb = parse_float(a), parse_float(b)
    if fa is not None and fb is not None:
        return abs(fa - fb) <= tol * max(abs(fa), abs(fb), 1.0)
    return a == b


def main():
    a_path, b_path, tol = sys.argv[1], sys.argv[2], float(sys.argv[3])
    with open(a_path, newline="") as fa, open(b_path, newline="") as fb:
        a_rows = list(csv.reader(fa))
        b_rows = list(csv.reader(fb))

    diffs = []
    if len(a_rows) != len(b_rows):
        diffs.append(f"row count: arneb={len(a_rows)} trino={len(b_rows)}")

    for r, (ra, rb) in enumerate(zip(a_rows, b_rows)):
        if len(ra) != len(rb):
            diffs.append(f"row {r}: col count arneb={len(ra)} trino={len(rb)}")
        else:
            for c, (va, vb) in enumerate(zip(ra, rb)):
                if not cells_equal(va, vb, tol):
                    diffs.append(f'row {r} col {c}: "{va}" vs "{vb}"')
                    if len(diffs) >= 20:
                        break
        if len(diffs) >= 20:
            break

    for d in diffs:
        print(d)
    sys.exit(1 if diffs else 0)


if __name__ == "__main__":
    main()
