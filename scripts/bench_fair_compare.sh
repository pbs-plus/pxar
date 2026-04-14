#!/bin/bash
# Fair benchmark: pxar-cli vs proxmox-backup-client
# Same container, same data, same PBS, wall-clock timing, best of 3
set -euo pipefail

CONTAINER="${1:-pbs-bench}"
FINGERPRINT="90:96:72:43:24:80:BF:4E:CE:3C:93:0E:99:4B:6D:29:CA:2D:CF:FE:EC:7C:A1:F0:42:A2:D5:7C:9A:41:08:57"
TOKEN="root@pam!bench:5a92bc15-a04c-4ebf-9720-2dcfdad55555"
REPOSITORY="root@pam@localhost:8007:bench-store"
RUNS=3

del() {
    docker exec "$CONTAINER" curl -s -X DELETE -k \
        -H "Authorization: PBSAPIToken=$TOKEN" \
        "https://localhost:8007/api2/json/admin/datastore/bench-store/groups?backup-type=host&backup-id=${1}" \
        >/dev/null 2>&1 || true
}

run_test() {
    local label="$1" id="$2" cmd="$3"
    local best_wall=99 best_dur=99
    for i in $(seq 1 $RUNS); do
        del "$id"
        sleep 0.2
        local s=$(date +%s%N)
        local out=$(docker exec "$CONTAINER" bash -c "$cmd" 2>&1) || true
        local e=$(date +%s%N)
        local wall=$(awk "BEGIN{printf \"%.3f\", ($e - $s) / 1000000000}")
        local dur=$(echo "$out" | grep -oP 'Duration: \K[\d.]+' || echo "99")
        if echo "$out" | grep -q "Duration:"; then
            if awk "BEGIN{exit ($wall < $best_wall) ? 0 : 1}"; then
                best_wall=$wall
                best_dur=$dur
            fi
        else
            echo "  [run $i failed: $out]" >&2
        fi
    done
    del "$id"
    printf "%-30s %-12s %-10s\n" "$label" "${best_wall}s" "${best_dur}s"
}

echo "================================================================"
echo "  Fair Benchmark: pxar-cli vs proxmox-backup-client"
echo "  Same container, same PBS, wall-clock, best of $RUNS"
echo "================================================================"
echo ""
printf "%-30s %-12s %-10s\n" "Test" "Wall (best)" "Duration"
printf "%-30s %-12s %-10s\n" "-----------------------------" "----------" "--------"

DATA="/tmp/bench-data/medium"
DATA_S="/tmp/bench-data/small"
DATA_L="/tmp/bench-data/large"

# --- pbc legacy/data/metadata (50×8KB) ---
run_test "pbc legacy (50f)" "pbc-med" \
    "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc.pxar:$DATA --repository $REPOSITORY --backup-id pbc-med --change-detection-mode legacy"

run_test "pbc data (50f)" "pbc-med-dat" \
    "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc.pxar:$DATA --repository $REPOSITORY --backup-id pbc-med-dat --change-detection-mode data"

# metadata needs previous backup
echo "  (initial data backup for pbc metadata...)"
docker exec "$CONTAINER" bash -c "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc-ref.pxar:$DATA --repository $REPOSITORY --backup-id pbc-med-ref --change-detection-mode data" 2>&1 | tail -1
run_test "pbc metadata (50f)" "pbc-med-meta" \
    "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc.pxar:$DATA --repository $REPOSITORY --backup-id pbc-med-meta --change-detection-mode metadata"
del "pbc-med-ref"

# --- pxar-cli legacy/data/metadata (50×8KB) ---
run_test "pxar legacy (50f)" "pxar-med" \
    "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-med --mode legacy $DATA"

run_test "pxar data (50f)" "pxar-med-dat" \
    "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-med-dat --mode data $DATA"

# metadata needs previous backup
echo "  (initial data backup for pxar metadata...)"
init_out=$(docker exec "$CONTAINER" bash -c "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-med-ref --mode data $DATA" 2>&1)
prev_time=$(echo "$init_out" | grep -oP 'BackupTime: \K\d+' || echo "")
echo "  (previous backup time: $prev_time)"
run_test "pxar metadata (50f)" "pxar-med-meta" \
    "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-med-meta --mode metadata --previous-backup-id pxar-med-ref --previous-backup-time $prev_time $DATA"
del "pxar-med-ref"

echo ""
echo "--- Small dataset (10×8KB ≈ 80KB) ---"

run_test "pbc legacy (10f)" "pbc-sm" \
    "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc.pxar:$DATA_S --repository $REPOSITORY --backup-id pbc-sm --change-detection-mode legacy"

run_test "pxar legacy (10f)" "pxar-sm" \
    "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-sm --mode legacy $DATA_S"

echo ""
echo "--- Large dataset (10×1MB ≈ 10MB) ---"

run_test "pbc legacy (10f-lg)" "pbc-lg" \
    "PBS_FINGERPRINT='$FINGERPRINT' PBS_PASSWORD=testpassword proxmox-backup-client backup pbc.pxar:$DATA_L --repository $REPOSITORY --backup-id pbc-lg --change-detection-mode legacy"

run_test "pxar legacy (10f-lg)" "pxar-lg" \
    "PBS_TOKEN='$TOKEN' PBS_FINGERPRINT='$FINGERPRINT' pxar-cli backup --repository $REPOSITORY --backup-id pxar-lg --mode legacy $DATA_L"

echo ""
echo "================================================================"
echo "pbc  = proxmox-backup-client"
echo "pxar = pxar-cli (Go library)"
echo "Wall = total wall-clock time (includes auth, startup)"
echo "Duration = self-reported backup duration"
echo "================================================================"