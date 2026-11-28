# File Connector

The file connector reads CSV and Parquet files from the local filesystem or an object store (`s3://`, `gs://`, `az://`; see [Object Storage](/connectors/object-store)).

## Parquet Files

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format that provides efficient compression and encoding. Arneb reads Parquet files natively using Apache Arrow.

### Configuration

```toml
[[tables]]
name = "lineitem"
path = "/data/tpch/lineitem.parquet"
format = "parquet"
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name used in SQL queries |
| `path` | string | Absolute path to the Parquet file |
| `format` | string | Must be `"parquet"` |

### Pushdown Support

Parquet files support all pushdown optimizations:

- **Filter pushdown**: row groups whose column statistics rule out a filter are skipped, and the remaining rows are filtered as they are decoded. This covers comparisons of a column with a literal, `IN` lists of literals, and the key sets a join learns at run time (dynamic filters). A filter on a column after a struct or map column is matched to the right leaf column. Filters still run above the scan as well, so a filter the reader cannot apply never changes the result.
- **Projection pushdown**: only requested columns are read from the file.
- **Limit pushdown**: stops reading after the required number of rows.

### Parallel Reads

A Parquet file is read as several splits, so one file can use every core
of a node and, in a cluster, every worker:

- The split count is the scan parallelism of the node that plans the query
  (its core count), capped so that each split covers at least 100,000
  rows. A smaller file, or one whose row count is unknown, is one split.
- When the file has at least as many row groups as splits, each split reads
  whole row groups, so no row group is read twice. Otherwise the file is cut
  into equal row ranges.
- Each split reserves memory for the largest row group it reads, not for
  the whole file.

### Example

```toml
[[tables]]
name = "orders"
path = "/data/orders.parquet"
format = "parquet"
```

```sql
SELECT order_id, total FROM orders WHERE total > 1000 LIMIT 10;
```

## CSV Files

Arneb can read CSV files with automatic schema inference.

### Configuration

```toml
[[tables]]
name = "users"
path = "/data/users.csv"
format = "csv"
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Table name used in SQL queries |
| `path` | string | Absolute path to the CSV file |
| `format` | string | Must be `"csv"` |

### Example

```toml
[[tables]]
name = "events"
path = "/data/events.csv"
format = "csv"
```

```sql
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
```

## Multiple Tables

Register multiple tables in a single config file:

```toml
[[tables]]
name = "lineitem"
path = "/data/tpch/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/tpch/orders.parquet"
format = "parquet"

[[tables]]
name = "customer"
path = "/data/tpch/customer.parquet"
format = "parquet"
```

```sql
SELECT c.name, COUNT(o.order_id)
FROM customer c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.name;
```
