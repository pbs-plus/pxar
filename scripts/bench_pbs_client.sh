#!/bin/bash
# Benchmark proxmox-backup-client against PBS in Docker
set -euo pipefail

CONTAINER="${1:-pbs-bench}"
FINGERPRINT="90:96:72:43:24:80:BF:4E:CE:3C:93:0E:99:4B:6D:29:CA:2D:CF:FE:EC:7C:A1:F0:42:A2:D5:7C:9A:41:08:57"
REPOSITORY="root@pam@localhost:8007:bench-store"
RUNS=5

delete_group() {
    local backup_id="$1"
    docker exec "$CONTAINER" curl -s -X DELETE -k \
        -H 'Authorization: PBSAPIToken=root@pam!bench:5a92bc15-a04c-4ebf-9720-2dcfdad55555' \
        "https://localhost:8007/api2/json/admin/datastore/bench-store/groups?backup-type=host&backup-id=${backup_id}" \
        >/dev/null 2>&1 || true
}

run_backup() {
    local path="$1"
    local backup_id="$2"
    
    # Use bash SECONDS for timing since 'time' is not available in container
    docker exec "$CONTAINER" bash -c '
        PBS_FINGERPRINT="'"$FINGERPRINT"'" \
        PBS_PASSWORD=testpassword \
        proxmox-backup-client backup '"${backup_id}"'.pxar:'"${path}"' \
            --repository '"$REPOSITORY"' \
            --backup-id '"${backup_id}"' 2>&1; echo "EXIT_CODE:$?"'
}

echo "=========================================="
echo " proxmox-backup-client Benchmark Results"
echo "=========================================="
echo ""
echo "Container: $CONTAINER"
echo "Runs per test: $RUNS (best time reported)"
echo ""

printf "%-12s %-10s %-12s %-12s %-15s\n" "Dataset" "Data Size" "Backup Size" "Best Wall" "Speed"
printf "%-12s %-10s %-12s %-12s %-15s\n" "--------" "---------" "------------" "---------" "-----"

declare -A DATASETS
DATASETS[small]="/tmp/bench-data/small"
DATASETS[medium]="/tmp/bench-data/medium"  
DATASETS[large]="/tmp/bench-data/large"
declare -A DATASETS_SIZES
DATASETS_SIZES[small]="80K"
DATASETS_SIZES[medium]="400K"
DATASETS_SIZES[large]="10M"

for name in small medium large; do
    path="${DATASETS[$name]}"
    size="${DATASETS_SIZES[$name]}"
    backup_id="pbc-${name}"
    
    best_wall=999999
    best_output=""
    
    for run in $(seq 1 $RUNS); do
        delete_group "$backup_id"
        sleep 0.3
        
        start=$(date +%s%N)
        output=$(run_backup "$path" "$backup_id" 2>&1)
        end=$(date +%s%N)
        
        wall_ms=$(( (end - start) / 1000000 ))
        wall_sec=$(awk "BEGIN{printf \"%.3f\", $wall_ms/1000}")
        
        exit_code=$(echo "$output" | grep -oP 'EXIT_CODE:\K\d+')
        if [[ "$exit_code" != "0" ]]; then
            echo "RUN FAILED: $name run $run"
            echo "$output" | tail -5
            continue
        fi
        
        if awk "BEGIN{exit ($wall_ms < $best_wall*1000 || $best_wall == 999999) ? 0 : 1}" 2>/dev/null; then
            best_wall=$wall_sec
            best_output="$output"
        fi
    done
    
    backup_size=$(echo "$best_output" | grep -oP 'had to backup \K[\d.]+ [KMG]iB' || echo "N/A")
    speed=$(echo "$best_output" | grep -oP 'average \K[\d.]+ [KMG]iB/s' || echo "N/A")
    
    printf "%-12s %-10s %-12s %-12s %-15s\n" "$name" "$size" "$backup_size" "${best_wall}s" "$speed"
    
    delete_group "$backup_id"
done

echo ""
echo "Note: proxmox-backup-client uses legacy (single-archive) mode only."
echo "      It always re-reads all file data (equivalent to DetectionLegacy)."