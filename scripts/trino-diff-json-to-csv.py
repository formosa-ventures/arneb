#!/usr/bin/env python3
"""Convert Trino's line-oriented JSON rows to unquoted comparison CSV."""
import json
import sys


def cell(value):
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


for line in sys.stdin:
    row = json.loads(line, object_pairs_hook=list)
    print(",".join(cell(value) for _, value in row))
