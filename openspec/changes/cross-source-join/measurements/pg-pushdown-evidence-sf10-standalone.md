| Query | PostgreSQL table | Columns | Remote WHERE | Statements |
|---|---|---|---|---|
| q02 | "tpch_mixed"."nation" | "n_nationkey", "n_name", "n_regionkey" | — | 1 |
| q02 | "tpch_mixed"."supplier" | "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal" | — | 1 |
| q02 | "tpch_mixed"."partsupp" | "ps_partkey", "ps_suppkey", "ps_supplycost" | — | 2 |
| q02 | "tpch_mixed"."part" | "p_partkey", "p_mfgr", "p_type", "p_size" | ("p_size" = 15) AND ("p_type" LIKE '%BRASS') | 5 |
| q02 | "tpch_mixed"."nation" | "n_nationkey", "n_regionkey" | — | 1 |
| q02 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | — | 1 |
| q02 | "tpch_mixed"."region" | "r_regionkey", "r_name" | ("r_name" = 'EUROPE') | 2 |
| q03 | "tpch_mixed"."customer" | "c_custkey", "c_mktsegment" | ("c_mktsegment" = 'BUILDING') | 4 |
| q05 | "tpch_mixed"."nation" | "n_nationkey", "n_name", "n_regionkey" | — | 1 |
| q05 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | — | 1 |
| q05 | "tpch_mixed"."customer" | "c_custkey" | — | 4 |
| q05 | "tpch_mixed"."region" | "r_regionkey", "r_name" | ("r_name" = 'ASIA') | 1 |
| q07 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | (("n_name" = 'FRANCE') OR ("n_name" = 'GERMANY')) | 1 |
| q07 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | ("s_nationkey" IN (6, 7)) AND ("s_nationkey" IN (7, 6)) | 1 |
| q07 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | (("n_name" = 'GERMANY') OR ("n_name" = 'FRANCE')) | 1 |
| q07 | "tpch_mixed"."customer" | "c_custkey", "c_nationkey" | ("c_nationkey" IN (6, 7)) AND ("c_nationkey" IN (6, 7)) | 4 |
| q08 | "tpch_mixed"."nation" | "n_nationkey", "n_regionkey" | — | 1 |
| q08 | "tpch_mixed"."customer" | "c_custkey", "c_nationkey" | — | 4 |
| q08 | "tpch_mixed"."part" | "p_partkey", "p_type" | ("p_type" = 'ECONOMY ANODIZED STEEL') | 5 |
| q08 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | — | 1 |
| q08 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | — | 1 |
| q08 | "tpch_mixed"."region" | "r_regionkey", "r_name" | ("r_name" = 'AMERICA') | 1 |
| q09 | "tpch_mixed"."partsupp" | "ps_partkey", "ps_suppkey", "ps_supplycost" | — | 1 |
| q09 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | — | 1 |
| q09 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | ("s_suppkey" BETWEEN 1 AND 100000) AND ("s_nationkey" IN (19, 18, 20, 6, 16, 2, 7, 9, 0, 14, 1, 15, 17, 21, 3, 12, 10, 22, 13, 11, 4, 24, 5, 8, 23)) AND ("s_nationkey" IN (0, 12, 2, 3, 1, 6, 21, 5, 11, 17, 22, 10, 23, 4, 16, 13, 14, 8, 24, 20, 15, 18, 7, 19, 9)) | 1 |
| q09 | "tpch_mixed"."part" | "p_partkey", "p_name" | ("p_name" LIKE '%green%') | 5 |
| q10 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | — | 1 |
| q10 | "tpch_mixed"."customer" | "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal" | ("c_nationkey" IN (3, 17, 13, 22, 24, 12, 4, 10, 16, 7, 23, 2, 1, 9, 0, 19, 20, 5, 18, 8, 11, 21, 15, 6, 14)) AND ("c_nationkey" IN (9, 8, 19, 7, 15, 3, 21, 6, 2, 18, 1, 4, 10, 24, 17, 23, 11, 14, 0, 16, 13, 5, 12, 22, 20)) | 4 |
| q11 | "tpch_mixed"."supplier" | "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment" | — | 1 |
| q11 | "tpch_mixed"."supplier" | "s_suppkey", "s_nationkey" | — | 2 |
| q11 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | — | 1 |
| q11 | "tpch_mixed"."partsupp" | "ps_suppkey", "ps_availqty", "ps_supplycost" | — | 1 |
| q11 | "tpch_mixed"."partsupp" | "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost" | — | 1 |
| q11 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | ("n_name" = 'GERMANY') | 1 |
| q13 | "tpch_mixed"."customer" | "c_custkey" | — | 4 |
| q14 | "tpch_mixed"."part" | "p_partkey", "p_type" | — | 5 |
| q15 | "tpch_mixed"."supplier" | "s_suppkey", "s_name", "s_address", "s_phone" | — | 1 |
| q16 | "tpch_mixed"."part" | "p_partkey", "p_brand", "p_type", "p_size" | ("p_brand" <> 'Brand#45') AND ("p_type" NOT LIKE 'MEDIUM POLISHED%') AND ("p_size" IN (49, 14, 23, 45, 19, 3, 36, 9)) | 5 |
| q16 | "tpch_mixed"."partsupp" | "ps_partkey", "ps_suppkey" | — | 1 |
| q17 | "tpch_mixed"."part" | "p_partkey", "p_brand", "p_container" | ("p_brand" = 'Brand#23') AND ("p_container" = 'MED BOX') | 5 |
| q18 | "tpch_mixed"."customer" | "c_custkey", "c_name" | — | 4 |
| q19 | "tpch_mixed"."part" | "p_partkey", "p_brand", "p_size", "p_container" | ("p_size" >= 1) AND ((("p_brand" = 'Brand#12') OR ("p_brand" = 'Brand#23')) OR ("p_brand" = 'Brand#34')) AND ((("p_container" IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')) OR ("p_container" IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) OR ("p_container" IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) AND (((("p_size" >= 1) AND ("p_size" <= 5)) OR (("p_size" >= 1) AND ("p_size" <= 10))) OR (("p_size" >= 1) AND ("p_size" <= 15))) | 5 |
| q20 | "tpch_mixed"."part" | "p_partkey", "p_name" | ("p_name" LIKE 'forest%') | 10 |
| q20 | "tpch_mixed"."partsupp" | "ps_partkey", "ps_suppkey", "ps_availqty" | ("ps_partkey" BETWEEN 5 AND 1999994) | 1 |
| q20 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | ("n_name" = 'CANADA') | 1 |
| q20 | "tpch_mixed"."supplier" | "s_suppkey", "s_name", "s_address", "s_nationkey" | ("s_suppkey" BETWEEN 2 AND 100000) | 1 |
| q21 | "tpch_mixed"."supplier" | "s_suppkey", "s_name", "s_nationkey" | — | 1 |
| q21 | "tpch_mixed"."nation" | "n_nationkey", "n_name" | ("n_name" = 'SAUDI ARABIA') | 1 |
| q22 | "tpch_mixed"."customer" | "c_phone", "c_acctbal" | — | 4 |
| q22 | "tpch_mixed"."customer" | "c_custkey", "c_phone", "c_acctbal" | — | 4 |
