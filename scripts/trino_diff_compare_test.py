"""Tests for trino-diff-compare.py.

Run: python3 -m unittest scripts/trino_diff_compare_test.py

An optional fourth argument names the ORDER BY key columns (1-based,
comma-separated). Rows whose key columns are equal may then appear in any
order relative to each other, because SQL leaves their order undefined.
Everything else must still match exactly: the same rows the same number of
times, and the same key sequence.
"""
import csv
import os
import subprocess
import sys
import tempfile
import unittest

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trino-diff-compare.py")


def compare(arneb, trino, keys=None, tol="1e-9"):
    """Write both row lists as CSV, run the comparator, return its exit code."""
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for name, rows in (("arneb.csv", arneb), ("trino.csv", trino)):
            path = os.path.join(d, name)
            with open(path, "w", newline="") as f:
                csv.writer(f).writerows(rows)
            paths.append(path)
        args = [sys.executable, SCRIPT, paths[0], paths[1], tol]
        if keys is not None:
            args.append(keys)
        return subprocess.run(args, capture_output=True, text=True).returncode


TRINO = [["a", "5"], ["b", "3"], ["c", "3"], ["d", "1"]]


class TieAwareCompare(unittest.TestCase):
    # Accepted: only rows tied on every key column trade places.

    def test_rows_tied_on_the_key_may_swap(self):
        arneb = [["a", "5"], ["c", "3"], ["b", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 0)

    def test_identical_output_still_matches(self):
        self.assertEqual(compare(TRINO, TRINO, keys="2"), 0)

    def test_tied_rows_still_compare_floats_with_tolerance(self):
        arneb = [["a", "5"], ["c", "3.0000000000001"], ["b", "3"], ["d", "1"]]
        trino = [["a", "5"], ["b", "3"], ["c", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, trino, keys="2"), 0)

    def test_rows_tied_on_all_of_several_keys_may_swap(self):
        trino = [["x", "1", "p"], ["x", "1", "q"], ["y", "2", "r"]]
        arneb = [["x", "1", "q"], ["x", "1", "p"], ["y", "2", "r"]]
        self.assertEqual(compare(arneb, trino, keys="1,2"), 0)

    # Rejected: anything SQL does define must still match exactly.

    def test_without_keys_a_swap_is_a_mismatch(self):
        arneb = [["a", "5"], ["c", "3"], ["b", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO), 1)

    def test_a_different_value_inside_a_tie_is_a_mismatch(self):
        arneb = [["a", "5"], ["c", "3"], ["x", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 1)

    def test_rows_with_different_keys_may_not_swap(self):
        arneb = [["a", "5"], ["b", "3"], ["d", "1"], ["c", "3"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 1)

    def test_a_different_key_order_is_a_mismatch(self):
        arneb = [["b", "3"], ["c", "3"], ["a", "5"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 1)

    def test_a_missing_row_is_a_mismatch(self):
        arneb = [["a", "5"], ["c", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 1)

    def test_an_extra_row_is_a_mismatch(self):
        arneb = [["a", "5"], ["c", "3"], ["b", "3"], ["e", "3"], ["d", "1"]]
        self.assertEqual(compare(arneb, TRINO, keys="2"), 1)

    def test_a_different_count_of_a_repeated_row_is_a_mismatch(self):
        trino = [["b", "3"], ["c", "3"], ["c", "3"]]
        arneb = [["b", "3"], ["b", "3"], ["c", "3"]]
        self.assertEqual(compare(arneb, trino, keys="2"), 1)

    def test_rows_tied_on_only_some_keys_may_not_swap(self):
        trino = [["x", "1", "p"], ["x", "2", "q"]]
        arneb = [["x", "2", "q"], ["x", "1", "p"]]
        self.assertEqual(compare(arneb, trino, keys="1,2"), 1)


if __name__ == "__main__":
    unittest.main()
