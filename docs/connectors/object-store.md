# Object Store Connector

Arneb reads CSV and Parquet files from cloud object stores using the [object_store](https://docs.rs/object_store) crate.

## Supported Stores

| Store | URL Scheme | Config Section |
|-------|------------|----------------|
| Amazon S3 | `s3://` | `[storage.s3]` |
| Google Cloud Storage | `gs://` | `[storage.gcs]` |
| Azure Blob Storage | `az://` | `[storage.azure]` |
| S3-compatible (MinIO, LocalStack) | `s3://` | `[storage.s3]` with `endpoint` |

## Amazon S3

### Configuration

```toml
[storage.s3]
region = "us-east-1"
# access_key_id = "AKIAIOSFODNN7EXAMPLE"
# secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[[tables]]
name = "events"
path = "s3://my-bucket/data/events.parquet"
format = "parquet"
```

### Credential Precedence

Arneb resolves S3 credentials in this order:

1. **Config file** — `access_key_id` and `secret_access_key` in `[storage.s3]`
2. **Environment variables** — `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
3. **IAM role / instance profile** — automatic on EC2, ECS, Lambda

::: tip
For production deployments, prefer IAM roles over static credentials. Omit `access_key_id` and `secret_access_key` from the config file to use the environment or IAM chain.
:::

### S3 Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `region` | string | yes | AWS region (e.g., `"us-east-1"`) |
| `endpoint` | string | no | Custom endpoint URL (for MinIO, LocalStack) |
| `allow_http` | boolean | no | Allow HTTP (non-TLS) connections. Required when endpoint uses `http://` |
| `access_key_id` | string | no | AWS access key. Falls back to env/IAM if omitted |
| `secret_access_key` | string | no | AWS secret key. Falls back to env/IAM if omitted |

## MinIO / LocalStack

For local development with S3-compatible services, set the `endpoint` and `allow_http` fields:

```toml
[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[[tables]]
name = "events"
path = "s3://warehouse/data/events.parquet"
format = "parquet"
```

## Google Cloud Storage

### Configuration

```toml
[storage.gcs]
service_account_path = "/path/to/service-account.json"

[[tables]]
name = "events"
path = "gs://my-bucket/data/events.parquet"
format = "parquet"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `service_account_path` | string | no | Path to GCP service account JSON key file |

## Per-Catalog Storage Overrides

When using catalogs (e.g., Hive), you can override storage settings per catalog:

```toml
[storage.s3]
region = "us-east-1"

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"

[catalogs.storage.s3]
region = "eu-west-1"
endpoint = "http://minio-eu:9000"
allow_http = true
```

The per-catalog settings merge with (and override) the global `[storage]` settings.
