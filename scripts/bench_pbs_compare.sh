#!/bin/bash
# Comprehensive benchmark: proxmox-backup-client vs pxar library
# Runs both tools against the same PBS instance with comparable data
set -euo pipefail

CONTAINER="${1:-pbs-bench}"
FINGERPRINT="90:96:72:43:24:80:BF:4E:CE:3C:93:0E:99:4B:6D:29:CA:2D:CF:FE:EC:7C:A1:F0:42:A2:D5:7C:9A:41:08:57"
TOKEN="root@pam!bench:5a92bc15-a04c-4ebf-9720-2dcfdad55555"
REPOSITORY="root@pam@localhost:8007:bench-store"
RUNS=5

delete_group() {
    local backup_id="$1"
    docker exec "$CONTAINER" curl -s -X DELETE -k \
        -H "Authorization: PBSAPIToken=$TOKEN" \
        "https://localhost:8007/api2/json/admin/datastore/bench-store/groups?backup-type=host&backup-id=${backup_id}" \
        >/dev/null 2>&1 || true
}

run_pbc_backup() {
    local path="$1"
    local backup_id="$2"
    
    docker exec "$CONTAINER" bash -c '
        PBS_FINGERPRINT="'"$FINGERPRINT"'" \
        PBS_PASSWORD=testpassword \
        proxmox-backup-client backup '"${backup_id}"'.pxar:'"${path}"' \
            --repository '"$REPOSITORY"' \
            --backup-id '"${backup_id}"' 2>&1'
}

echo "================================================================"
echo "  PBS Backup Benchmark: proxmox-backup-client vs pxar library"
echo "================================================================"
echo ""
echo "Environment: PBS Docker container ($CONTAINER), localhost network"
echo "Runs per test: $RUNS (best wall-clock time reported)"
echo ""

# ---- Part 1: proxmox-backup-client ----
echo "--- proxmox-backup-client (legacy mode only) ---"
echo ""

printf "%-18s %-10s %-12s %-12s %-18s %-10s\n" "Dataset" "Data Size" "Backup Size" "Best Wall" "Speed" "Compress"
printf "%-18s %-10s %-12s %-12s %-18s %-10s\n" "------------------" "---------" "------------" "---------" "------------------" "-----"

declare -A PBC_DATASETS
PBC_DATASETS[small]="/tmp/bench-data/small"
PBC_DATASETS[medium]="/tmp/bench-data/medium"
PBC_DATASETS[large]="/tmp/bench-data/large"
PBC_DATASETS[50files]="/tmp/bench-data/pbc-50files"

declare -A PBC_SIZES
PBC_SIZES[small]="80K (10 files)"
PBC_SIZES[medium]="400K (50 files)"
PBC_SIZES[large]="10M (10 files)"
PBC_SIZES[50files]="400K (50×8KB files)"

for name in small medium large 50files; do
    path="${PBC_DATASETS[$name]}"
    size="${PBC_SIZES[$name]}"
    backup_id="pbc-${name}"
    
    best_wall=999999
    best_output=""
    
    for run in $(seq 1 $RUNS); do
        delete_group "$backup_id"
        sleep 0.3
        
        start=$(date +%s%N)
        output=$(run_pbc_backup "$path" "$backup_id" 2>&1)
        end=$(date +%s%N)
        
        wall_ms=$(( (end - start) / 1000000 ))
        wall_sec=$(awk "BEGIN{printf \"%.3f\", $wall_ms/1000}")
        
        if echo "$output" | grep -q "Duration:"; then
            if awk "BEGIN{exit ($wall_sec < $best_wall || $best_wall == 999999) ? 0 : 1}" 2>/dev/null; then
                best_wall=$wall_sec
                best_output="$output"
            fi
        fi
    done
    
    backup_size=$(echo "$best_output" | grep -oP 'had to backup \K[\d.]+ [KMG]iB' || echo "N/A")
    compress_size=$(echo "$best_output" | grep -oP 'compressed \K[\d.]+ [KMG]iB' || echo "N/A")
    speed=$(echo "$best_output" | grep -oP 'average \K[\d.]+ [KMG]iB/s' || echo "N/A")
    
    printf "%-18s %-10s %-12s %-12s %-18s %-10s\n" "$name" "$size" "$backup_size" "${best_wall}s" "$speed" "$compress_size"
    
    delete_group "$backup_id"
done

echo ""
echo "--- pxar library (Go benchmarks, same PBS instance) ---"
echo ""
echo "Test data: 50 files × 8KB = 400KB per backup"
echo ""

# Clean up old benchmark snapshots
for id in bench-leg bench-data bench-meta-prev bench-meta-curr bench-raw bench-split; do
    for i in 0 1 2; do
        delete_group "${id}-${i}"
    done
done

echo "Running Go PBS benchmarks (3 iterations each, best of 3)..."
echo ""

PBS_URL=https://localhost:8007/api2/json PBS_DATASTORE=bench-store PBS_TOKEN="$TOKEN" \
    go test -tags=integration -run='^$' \
    -bench='BenchmarkPBS(Legacy|Data|Metadata|UploadRaw|UploadSplitRaw)' \
    -benchtime=3x -count=3 ./backupproxy/ 2>&1 | \
    grep -E '^Benchmark|^ok|^FAIL' || true

echo ""
echo "================================================================"
echo "  Summary"
echo "================================================================"
echo ""
echo "proxmox-backup-client: Uses legacy (single-archive) mode."
echo "  - All file data is re-read and re-uploaded every backup"
echo "  - No incremental detection support"
echo ""
echo "pxar library: Supports 3 detection modes:"
echo "  - DetectionLegacy: Full re-read (same as pbc)"
echo "  - DetectionData:   Split metadata/payload, full re-read"
echo "  - DetectionMetadata: Incremental via metadata comparison,"
echo "                       reuses chunks from previous backup"
echo ""
echo "To run comparable pxar benchmarks:"
echo "  PBS_URL=https://localhost:8007/api2/json \\"
echo "  PBS_DATASTORE=bench-store \\"
echo "  PBS_TOKEN='root@pam!bench:5a92bc15-a04c-4ebf-9720-2dcfdad55555' \\"
echo "  go test -tags=integration -bench=BenchmarkPBS ./backupproxy/"