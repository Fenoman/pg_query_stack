#!/usr/bin/env bash
# bench/run_micro.sh — DO-loop микробенч (ns/iter) в трёх режимах.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

PG_BIN="${PG_BIN:-/home/bench/pg16rel/bin}"
PG_DATA="${PG_DATA:-/home/bench/pg16rel-bench-data}"
PG_PORT="${PG_PORT:-55444}"
PG_DB="${PG_DB:-postgres}"
RUNS="${RUNS:-7}"
ITERS="${ITERS:-200000}"

PSQL="$PG_BIN/psql -p $PG_PORT -d $PG_DB -q"
PGCTL="$PG_BIN/pg_ctl -D $PG_DATA"

# bench.iters — кастомный GUC для передачи N в DO-блок (psql-переменные
# `:iters` не подставляются внутри DO-string-literal).
# Custom variables нужно объявить в postgresql.conf через
# session_preload_libraries или через ALTER SYSTEM custom_variable_classes,
# но в PG 9.2+ ANY name with a dot is allowed as user-defined GUC.
# Передаём прямо через -c "SET bench.iters TO N".

ts="$(date +%Y%m%d_%H%M%S)"
csv="$RESULTS_DIR/micro_${ts}.csv"
echo "mode,run,ns_per_iter" > "$csv"

run_mode() {
    local mode="$1"
    local i out ns

    echo "[micro] mode=$mode runs=$RUNS iters=$ITERS"
    for ((i=1; i<=RUNS; i++)); do
        out="$($PSQL -c "SET bench.iters TO $ITERS" -f "$SCRIPT_DIR/scripts/inproc_microbench.sql" 2>&1)"
        ns="$(echo "$out" | grep -oP 'NS_PER_ITER=\K[0-9.]+' || true)"
        if [[ -z "$ns" ]]; then
            echo "[micro] ERROR: could not parse ns/iter from output:"; echo "$out"; exit 1
        fi
        echo "$mode,$i,$ns" | tee -a "$csv"
    done
}

# Освобождаем ВСЕ существующие backend'ы между сменами preload —
# чтобы новый preload вступил в силу для свежих коннектов.
kill_backends() {
    $PG_BIN/psql -p "$PG_PORT" -d "$PG_DB" -tAc \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type='client backend';" >/dev/null 2>&1 || true
}

# Mode 1: no preload (baseline)
$PG_BIN/psql -p "$PG_PORT" -d "$PG_DB" -tAc "ALTER SYSTEM RESET session_preload_libraries;" >/dev/null
$PGCTL reload >/dev/null
kill_backends; sleep 0.3
run_mode "no_preload"

# Mode 2: preload + enabled=on
$PG_BIN/psql -p "$PG_PORT" -d "$PG_DB" -tAc "ALTER SYSTEM SET session_preload_libraries='pg_query_stack';" >/dev/null
$PGCTL reload >/dev/null
kill_backends; sleep 0.3
run_mode "enabled_on"

# Mode 3: preload + enabled=off (через PGOPTIONS)
echo "[micro] mode=enabled_off runs=$RUNS iters=$ITERS"
for ((i=1; i<=RUNS; i++)); do
    out="$(PGOPTIONS='-c pg_query_stack.enabled=off' $PSQL -c "SET bench.iters TO $ITERS" -f "$SCRIPT_DIR/scripts/inproc_microbench.sql" 2>&1)"
    ns="$(echo "$out" | grep -oP 'NS_PER_ITER=\K[0-9.]+' || true)"
    if [[ -z "$ns" ]]; then
        echo "[micro] ERROR (enabled_off): $out"; exit 1
    fi
    echo "enabled_off,$i,$ns" | tee -a "$csv"
done

# Печатаем сводку: медиана, мин/макс по каждому mode
python3 - <<EOF
import csv, statistics, sys
rows = list(csv.DictReader(open("$csv")))
modes = sorted(set(r["mode"] for r in rows))
print()
print("|       mode       | runs | median ns/iter |  min  |  max  |")
print("|------------------|------|----------------|-------|-------|")
for m in modes:
    vals = sorted(float(r["ns_per_iter"]) for r in rows if r["mode"] == m)
    print(f"| {m:<16} | {len(vals):4d} |  {statistics.median(vals):12.2f}  | {min(vals):5.1f} | {max(vals):5.1f} |")
print()
print("Δ enabled_on - no_preload =", end=" ")
no = statistics.median(float(r["ns_per_iter"]) for r in rows if r["mode"]=="no_preload")
on = statistics.median(float(r["ns_per_iter"]) for r in rows if r["mode"]=="enabled_on")
print(f"{on-no:.1f} ns/query  (target ≤ 60 ns)")
EOF

echo "[micro] csv saved: $csv"
