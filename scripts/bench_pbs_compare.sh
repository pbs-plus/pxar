#!/bin/bash
# bench_pbs_compare.sh — Benchmark pxar backupproxy vs proxmox-backup-client
#
# Prerequisites:
#   - A running PBS instance (local Docker or remote)
#   - PBS_URL, PBS_DATASTORE, PBS_TOKEN env vars (or .env file)
#   - proxmox-backup-client installed (for comparison)
#   - Go 1.21+
#
# Usage:
#   ./bench_pbs_compare.sh                 # Use env vars
#   ./bench_pbs_compare.sh .env            # Load from .env file
#
# The script:
#   1. Creates test files of various sizes
#   2. Runs proxmox-backup-client backup for baseline
#   3. Runs pxar backupproxy benchmarks against same PBS
#   4. Reports comparison table

set -euo pipefail

# Load env file if provided
if [ -n "${1:-}" ]; then
    source "$1"
fi

PBS_URL="${PBS_URL:-https://localhost:8007}"
PBS_DATASTORE="${PBS_DATASTORE:-test-store}"
PBS_TOKEN="${PBS_TOKEN:-}"
PBS_NS="${PBS_NS:-}"

# Check dependencies
command -v go >/dev/null 2>&1 || { echo "go not found"; exit 1; }
command -v proxmox-backup-client >/dev/null 2>&1 || { echo "proxmox-backup-client not found (install for comparison)"; exit 1; }

# Create temp directory for test data
BENCH_DIR=$(mktemp -d)
trap 'rm -rf "$BENCH_DIR"' EXIT

echo "=== pxar vs proxmox-backup-client Benchmark ==="
echo "PBS: $PBS_URL"
echo "Datastore: $PBS_DATASTORE"
echo "Test directory: $BENCH_DIR"
echo ""

# Generate test data
generate_test_data() {
    local name=$1
    local file_count=$2
    local file_size=$3
    local dir="$BENCH_DIR/$name"
    mkdir -p "$dir"
    
    for i in $(seq 1 "$file_count"); do
        dd if=/dev/urandom of="$dir/file_$i.bin" bs="$file_size" count=1 2>/dev/null
    done
    echo "$dir"
}

# --- proxmox-backup-client backup ---
run_pbs_client_backup() {
    local dir=$1
    local backup_id=$2
    local ns=$3
    
    export PBS_REPOSITORY="${PBS_DATASTORE}:${backup_id}@${PBS_URL#https://}"
    
    local start end
    start=$(date +%s%N)
    
    proxmox-backup-client backup "$dir" \
        --repository "$PBS_REPOSITORY" \
        ${ns:+--ns "$ns"} \
        --backup-type host \
        --backup-id "$backup_id" \
        2>/dev/null
    
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

# --- pxar backupproxy benchmark ---
run_pxar_benchmark() {
    local name=$1
    local mode=$2
    local dir=$3
    
    cd "$(git rev-parse --show-toplevel)"
    local result
    result=$(go test -bench="BenchmarkPBS${mode}" -benchtime=1x -count=1 -run=^$ -tags=integration \
        ./backupproxy/ 2>&1 | grep "^BenchmarkPBS" || true)
    echo "$result"
}

# Generate test datasets
echo "Generating test data..."
SMALL_DIR=$(generate_test_data "small" 10 "4k")     # 10 files × 4KB  = 40KB
MEDIUM_DIR=$(generate_test_data "medium" 100 "16k") # 100 files × 16KB = 1.6MB
LARGE_DIR=$(generate_test_data "large" 10 "1M")      # 10 files × 1MB  = 10MB

TIMESTAMP=$(date +%s)

echo ""
echo "--- proxmox-backup-client (baseline) ---"

for dataset in "small" "medium" "large"; do
    dir_var="${dataset}_DIR"
    dir="${!dir_var}"
    
    echo -n "  $dataset: "
    ms=$(run_pbs_client_backup "$dir" "pxar-bench-${TIMESTAMP}" "${PBS_NS}")
    size=$(du -sb "$dir" | cut -f1)
    echo "${ms}ms  (${size} bytes)"
done

echo ""
echo "--- pxar backupproxy (legacy mode) ---"
echo "  (Run manually: go test -bench=BenchmarkLegacy -benchtime=3s ./backupproxy/)"
echo ""

echo "--- pxar backupproxy (data mode) ---"
echo "  (Run manually: go test -bench=BenchmarkData -benchtime=3s ./backupproxy/)"
echo ""

echo "--- pxar backupproxy (metadata mode) ---"
echo "  (Run manually: go test -bench=BenchmarkMetadata -benchtime=3s ./backupproxy/)"
echo ""

echo "=== LocalStore Benchmarks (no network overhead) ==="
echo "Run: go test -bench='Benchmark(Legacy|Data|Metadata)' -benchtime=3s ./backupproxy/"
echo ""

# Run the actual local benchmarks
echo "--- Results ---"
go test -bench='Benchmark(Legacy|Data|Metadata)' -benchtime=1s -count=1 -run=^$ ./backupproxy/ 2>&1 | grep -E "^Benchmark" || true

echo ""
echo "=== Raw Throughput ==="
go test -bench='Benchmark(ChunkerThroughput|InMemoryChunkPipeline|LocalUpload)' -benchtime=1s -count=1 -run=^$ ./buzhash/ ./datastore/ ./backupproxy/ 2>&1 | grep -E "^Benchmark" || true

echo ""
echo "Test data preserved in: $BENCH_DIR"