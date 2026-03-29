#!/usr/bin/env bash
#
# Generate TPC-H test data using dbgen.
#
# Prerequisites:
#   - dbgen compiled and in PATH (https://github.com/electrum/tpch-dbgen)
#   - Or: brew install tpch-dbgen (if available)
#
# Usage:
#   ./benchmarks/tpch/scripts/generate_data.sh [scale_factor]
#   # Default scale factor: 1 (1GB)

set -euo pipefail

SF="${1:-1}"
DATA_DIR="benchmarks/tpch/data/sf${SF}"

echo "Generating TPC-H data (SF=${SF})..."
echo "Output directory: ${DATA_DIR}"

mkdir -p "$DATA_DIR"

if ! command -v dbgen &>/dev/null; then
    echo "ERROR: dbgen not found in PATH."
    echo ""
    echo "Install TPC-H dbgen:"
    echo "  git clone https://github.com/electrum/tpch-dbgen.git"
    echo "  cd tpch-dbgen && make"
    echo "  export PATH=\$PATH:\$(pwd)"
    echo ""
    echo "Or generate sample CSV data with:"
    echo "  ./benchmarks/tpch/scripts/generate_sample.sh"
    exit 1
fi

# Generate data using dbgen.
cd "$DATA_DIR"
dbgen -s "$SF" -f

# Rename files to .csv (dbgen uses .tbl extension).
for f in *.tbl; do
    mv "$f" "${f%.tbl}.csv"
done

echo ""
echo "Generated files:"
ls -lh *.csv

echo ""
echo "Done. Data in: ${DATA_DIR}"
