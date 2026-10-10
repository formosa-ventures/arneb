# Hive test fixtures

## `trino_types.orc`

A data file written by Trino's Hive connector (default storage format ORC,
ZLIB compression) against the local docker-compose stack, then copied out of
RustFS unchanged:

```sql
CREATE TABLE hive.orc_extra.types AS SELECT * FROM (VALUES
  (TINYINT '-128', SMALLINT '-32768', -2147483648, BIGINT '-9223372036854775808',
   REAL '-1.5', -2.25e10, false, '', 'é漢字', X'00ff',
   DECIMAL '-9999999999.99', DECIMAL '-12345678901234567890.0123456789',
   DATE '1900-01-01', TIMESTAMP '1969-12-31 23:59:59.999'),
  (TINYINT '127', SMALLINT '32767', 2147483647, BIGINT '9223372036854775807',
   REAL '3.25', 1e-300, true, 'hello', 'v', X'',
   DECIMAL '0.01', DECIMAL '1.0000000001',
   DATE '2024-02-29', TIMESTAMP '2024-03-10 02:30:00.123'),
  (TINYINT '0', SMALLINT '0', 0, BIGINT '0', REAL '0', 0e0, true, 'x', 'y', X'41',
   DECIMAL '42.00', DECIMAL '0', DATE '1970-01-01', TIMESTAMP '2262-04-11 23:47:16.854'),
  (NULL, ...)   -- one all-NULL row
) AS t(tiny, small, i, big, r, d, b, s, v, bin, dec, bigdec, day, ts);
```

Column types: `tinyint, smallint, integer, bigint, real, double, boolean,
varchar, varchar, varbinary, decimal(12,2), decimal(30,10), date,
timestamp(3)`. The expected values in `crates/hive/src/scan_tests.rs` are the
ones `SELECT * FROM hive.orc_extra.types` returns in Trino.
