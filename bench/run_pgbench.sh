#!/usr/bin/env bash
# bench/run_pgbench.sh — pgbench -M prepared в трёх режимах × три сценария.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

PG_BIN="${PG_BIN:-/home/bench/pg16rel/bin}"
PG_DATA="${PG_DATA:-/home/bench/pg16rel-bench-data}"
PG_PORT="${PG_PORT:-55444}"
PG_DB="${PG_DB:-postgres}"
RUNS="${RUNS:-5}"
ITERS="${ITERS:-50000}"

PSQL="$PG_BIN/psql -p $PG_PORT -d $PG_DB"
PGBENCH="$PG_BIN/pgbench -p $PG_PORT -d $PG_DB -n -c 1 -t $ITERS -M prepared"
PGCTL="$PG_BIN/pg_ctl -D $PG_DATA"

ts="$(date +%Y%m%d_%H%M%S)"
csv="$RESULTS_DIR/pgbench_${ts}.csv"
echo "script,mode,run,tps,latency_ms" > "$csv"

# Подготовка вспомогательных объектов (одноразово на инстанс)
$PSQL -q -f "$SCRIPT_DIR/scripts/_setup_audit.sql" >/dev/null

kill_backends() {
    $PSQL -tAc \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type='client backend';" >/dev/null 2>&1 || true
}

run_pgbench() {
    local script="$1"
    local mode="$2"
    local pgopts="${3:-}"
    local i tps lat out

    echo "[pgbench] script=$script mode=$mode runs=$RUNS iters=$ITERS"
    for ((i=1; i<=RUNS; i++)); do
        out="$(PGOPTIONS="$pgopts" $PGBENCH -f "$SCRIPT_DIR/scripts/$script" 2>&1)"
        tps="$(echo "$out" | grep -oP 'tps = \K[0-9.]+' | head -1)"
        lat="$(echo "$out" | grep -oP 'latency average = \K[0-9.]+' | head -1)"
        if [[ -z "$tps" ]]; then
            echo "[pgbench] ERROR parsing pgbench output:"; echo "$out"; exit 1
        fi
        echo "$script,$mode,$i,$tps,$lat" | tee -a "$csv"
    done
}

# Mode 1: no preload
$PSQL -tAc "ALTER SYSTEM RESET session_preload_libraries;" >/dev/null
$PGCTL reload >/dev/null
kill_backends; sleep 0.3
for s in sel1.sql nested_function.sql audit_insert.sql; do
    run_pgbench "$s" "no_preload"
done

# Mode 2: preload + enabled=on
$PSQL -tAc "ALTER SYSTEM SET session_preload_libraries='pg_query_stack';" >/dev/null
$PGCTL reload >/dev/null
kill_backends; sleep 0.3
for s in sel1.sql nested_function.sql audit_insert.sql; do
    run_pgbench "$s" "enabled_on"
done

# Mode 3: preload + enabled=off
for s in sel1.sql nested_function.sql audit_insert.sql; do
    run_pgbench "$s" "enabled_off" "-c pg_query_stack.enabled=off"
done

# Сводка: median TPS / latency по script×mode
python3 - <<EOF
import csv, statistics
rows = list(csv.DictReader(open("$csv")))
scripts = sorted(set(r["script"] for r in rows))
modes = ["no_preload", "enabled_on", "enabled_off"]
print()
print("|         script          |    mode     | median TPS | median lat ms |")
print("|-------------------------|-------------|------------|---------------|")
for s in scripts:
    for m in modes:
        vals_tps = [float(r["tps"]) for r in rows if r["script"]==s and r["mode"]==m]
        vals_lat = [float(r["latency_ms"]) for r in rows if r["script"]==s and r["mode"]==m]
        if not vals_tps:
            continue
        print(f"| {s:<23} | {m:<11} | {statistics.median(vals_tps):10.0f} | {statistics.median(vals_lat):13.4f} |")
print()
EOF

echo "[pgbench] csv saved: $csv"
