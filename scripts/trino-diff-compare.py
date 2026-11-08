#!/usr/bin/env python3
"""Float-tolerant CSV diff between an Arneb and a Trino result.

Usage: trino-diff-compare.py ARNEB.csv TRINO.csv TOL [ORDER_KEYS]
Exit 0 when every cell matches; exit 1 and print up to 20 mismatches otherwise.
ORDER_KEYS is a comma-separated list of 1-based columns whose ties may reorder.
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


def rows_equal(a, b, tol):
    return len(a) == len(b) and all(cells_equal(x, y, tol) for x, y in zip(a, b))


def align_ties(a_rows, b_rows, keys, tol):
    def same_keys(a, b):
        return all(k < len(a) and k < len(b) and cells_equal(a[k], b[k], tol) for k in keys)

    if len(a_rows) != len(b_rows) or not all(
        same_keys(a, b) for a, b in zip(a_rows, b_rows)
    ):
        return b_rows

    aligned = b_rows[:]
    start = 0
    while start < len(b_rows):
        end = start + 1
        while end < len(b_rows) and same_keys(b_rows[start], b_rows[end]):
            end += 1

        matched = {}

        def match(a_index, seen):
            for b_index in range(start, end):
                if b_index in seen or not rows_equal(a_rows[a_index], b_rows[b_index], tol):
                    continue
                seen.add(b_index)
                if b_index not in matched or match(matched[b_index], seen):
                    matched[b_index] = a_index
                    return True
            return False

        if all(match(a_index, set()) for a_index in range(start, end)):
            for b_index, a_index in matched.items():
                aligned[a_index] = b_rows[b_index]
        start = end
    return aligned


def main():
    a_path, b_path, tol = sys.argv[1], sys.argv[2], float(sys.argv[3])
    with open(a_path, newline="") as fa, open(b_path, newline="") as fb:
        a_rows = list(csv.reader(fa))
        b_rows = list(csv.reader(fb))

    if len(sys.argv) > 4:
        keys = [int(k) - 1 for k in sys.argv[4].split(",")]
        b_rows = align_ties(a_rows, b_rows, keys, tol)

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
