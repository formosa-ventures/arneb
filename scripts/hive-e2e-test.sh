#!/usr/bin/env bash
#
# Hive + MinIO E2E test for Arneb.
#
# Prerequisites:
#   docker compose up -d   (starts HMS + MinIO)
#   cargo build --release  (builds arneb binary)
#
# This script:
#   1. Creates test Parquet data and uploads to MinIO
#   2. Registers a table in Hive Metastore via beeline
#   3. Starts Arneb with Hive catalog config
#   4. Queries the Hive table through Arneb via psql
#   5. Cleans up
#
# Usage:
#   ./scripts/hive-e2e-test.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ARNEB_BIN="${PROJECT_DIR}/target/release/arneb"
ARNEB_PORT=15432
ARNEB_PID=""
MINIO_ENDPOINT="http://localhost:9000"
MINIO_BUCKET="warehouse"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

cleanup() {
    if [ -n "$ARNEB_PID" ] && kill -0 "$ARNEB_PID" 2>/dev/null; then
        echo -e "${YELLOW}Stopping Arneb (PID $ARNEB_PID)...${NC}"
        kill "$ARNEB_PID" 2>/dev/null || true
        wait "$ARNEB_PID" 2>/dev/null || true
    fi
    rm -f /tmp/arneb-e2e-config.toml /tmp/arneb-e2e-test.parquet
}
trap cleanup EXIT

echo "============================================"
echo " Arneb Hive + MinIO E2E Test"
echo "============================================"

# --- Check prerequisites ---
echo -e "\n${YELLOW}[1/6] Checking prerequisites...${NC}"

if ! command -v mc &>/dev/null; then
    echo -e "${RED}Error: MinIO client (mc) not found. Install: brew install minio/stable/mc${NC}"
    exit 1
fi

if ! command -v psql &>/dev/null; then
    echo -e "${RED}Error: psql not found. Install: brew install postgresql${NC}"
    exit 1
fi

if [ ! -f "$ARNEB_BIN" ]; then
    echo -e "${RED}Error: Arneb binary not found at $ARNEB_BIN${NC}"
    echo "Run: cargo build --release"
    exit 1
fi

# Check docker services
if ! docker compose ps --status running 2>/dev/null | grep -q minio; then
    echo -e "${RED}Error: MinIO not running. Run: docker compose up -d${NC}"
    exit 1
fi

if ! docker compose ps --status running 2>/dev/null | grep -q hive-metastore; then
    echo -e "${RED}Error: Hive Metastore not running. Run: docker compose up -d${NC}"
    exit 1
fi

echo -e "${GREEN}All prerequisites met.${NC}"

# --- Create test Parquet data ---
echo -e "\n${YELLOW}[2/6] Creating test Parquet data...${NC}"

python3 -c "
import struct, io

# Minimal approach: use pyarrow if available, otherwise skip
try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        'id': pa.array([1, 2, 3], type=pa.int32()),
        'name': pa.array(['Alice', 'Bob', 'Carol'], type=pa.string()),
        'score': pa.array([95.5, 87.3, 92.1], type=pa.float64()),
    })
    pq.write_table(table, '/tmp/arneb-e2e-test.parquet')
    print('Created test Parquet file with pyarrow')
except ImportError:
    print('ERROR: pyarrow not installed. Install: pip install pyarrow')
    exit(1)
"

# --- Upload to MinIO ---
echo -e "\n${YELLOW}[3/6] Uploading Parquet to MinIO...${NC}"

mc alias set arneb-minio "$MINIO_ENDPOINT" minioadmin minioadmin --api S3v4 2>/dev/null
mc cp /tmp/arneb-e2e-test.parquet arneb-minio/${MINIO_BUCKET}/default/students/data.parquet
echo -e "${GREEN}Uploaded to s3://${MINIO_BUCKET}/default/students/data.parquet${NC}"

# --- Register table in HMS ---
echo -e "\n${YELLOW}[4/6] Registering table in Hive Metastore...${NC}"

# Use beeline inside the HMS container to create the external table
docker compose exec -T hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://" -e "
CREATE DATABASE IF NOT EXISTS test_db;
DROP TABLE IF EXISTS test_db.students;
CREATE EXTERNAL TABLE test_db.students (
    id INT,
    name STRING,
    score DOUBLE
)
STORED AS PARQUET
LOCATION 's3a://${MINIO_BUCKET}/default/students';
DESCRIBE test_db.students;
" 2>&1 | tail -20

echo -e "${GREEN}Table test_db.students registered in HMS.${NC}"

# --- Start Arneb ---
echo -e "\n${YELLOW}[5/6] Starting Arneb server...${NC}"

cat > /tmp/arneb-e2e-config.toml <<EOF
bind_address = "127.0.0.1"
port = ${ARNEB_PORT}

[storage.s3]
region = "us-east-1"
endpoint = "${MINIO_ENDPOINT}"
allow_http = true

[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"
default_schema = "test_db"
EOF

AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
"$ARNEB_BIN" --config /tmp/arneb-e2e-config.toml &
ARNEB_PID=$!

# Wait for Arneb to be ready
echo "Waiting for Arneb to start (PID $ARNEB_PID)..."
for i in $(seq 1 30); do
    if psql -h 127.0.0.1 -p $ARNEB_PORT -U test -d test -c "SELECT 1" &>/dev/null; then
        echo -e "${GREEN}Arneb is ready.${NC}"
        break
    fi
    if ! kill -0 "$ARNEB_PID" 2>/dev/null; then
        echo -e "${RED}Arneb process died.${NC}"
        exit 1
    fi
    sleep 1
done

# --- Run queries ---
echo -e "\n${YELLOW}[6/6] Running test queries...${NC}"

echo "--- Query: SELECT * FROM datalake.test_db.students ---"
RESULT=$(psql -h 127.0.0.1 -p $ARNEB_PORT -U test -d test -t -A -c \
    "SELECT id, name, score FROM datalake.test_db.students ORDER BY id" 2>&1)

echo "$RESULT"

# Verify results
EXPECTED_ROWS=3
ACTUAL_ROWS=$(echo "$RESULT" | grep -c "|" || true)

if [ "$ACTUAL_ROWS" -eq "$EXPECTED_ROWS" ]; then
    echo -e "\n${GREEN}============================================${NC}"
    echo -e "${GREEN} E2E TEST PASSED: $ACTUAL_ROWS/$EXPECTED_ROWS rows returned${NC}"
    echo -e "${GREEN}============================================${NC}"
else
    echo -e "\n${RED}============================================${NC}"
    echo -e "${RED} E2E TEST FAILED: expected $EXPECTED_ROWS rows, got $ACTUAL_ROWS${NC}"
    echo -e "${RED}============================================${NC}"
    exit 1
fi
