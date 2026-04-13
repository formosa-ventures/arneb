# File Connector

The file connector reads CSV and Parquet files from the local filesystem.

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

- **Filter pushdown** — row group pruning based on column statistics
- **Projection pushdown** — only requested columns are read from the file
- **Limit pushdown** — stops reading after the required number of rows

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
