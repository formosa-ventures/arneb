-- TPC-H Q15: Top Supplier.
--
-- Spec form uses `WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue0)`,
-- which is FP non-deterministic across parallel SUM orderings — the outer
-- `total_revenue` and the subquery's `MAX(total_revenue)` may differ by ULPs
-- when computed in different aggregation orders, so the `=` filter can drop
-- the validation row (supplier 8449) on some engines / runs.
--
-- We use `>= MAX - 0.01` instead. The TPC-H spec already permits a 0.01
-- tolerance on monetary cell comparisons, so this preserves the intended
-- semantics (top-revenue supplier) while being deterministic across engines.
WITH revenue0 AS (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
    GROUP BY l_suppkey
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier
JOIN revenue0 ON s_suppkey = supplier_no
WHERE total_revenue >= (
        SELECT MAX(total_revenue) - 0.01
        FROM revenue0
    )
ORDER BY s_suppkey
