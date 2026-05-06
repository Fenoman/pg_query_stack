#!/usr/bin/env bash
# bench/run_perf.sh — perf record + report + stat + flamegraph для одного
# горячего backend'а в трёх режимах. Запускать на Linux (perf обязателен).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

PG_BIN="${PG_BIN:-/home/bench/pg16rel/bin}"
PG_DATA="${PG_DATA:-/home/bench/pg16rel-bench-data}"
PG_PORT="${PG_PORT:-55444}"
PG_DB="${PG_DB:-postgres}"
PERF_DURATION="${PERF_DURATION:-30}"
PERF_FREQ="${PERF_FREQ:-999}"
ITERS_PERF="${ITERS_PERF:-50000000}"   # достаточно чтобы покрыть PERF_DURATION
ITERS_STAT="${ITERS_STAT:-200000}"     # для perf stat

PSQL="$PG_BIN/psql -p $PG_PORT -d $PG_DB"
PGCTL="$PG_BIN/pg_ctl -D $PG_DATA"
FG_DIR="$SCRIPT_DIR/tools/FlameGraph"

if ! command -v perf >/dev/null; then
    echo "[perf] ERROR: perf not found. Install linux-tools-common / linux-tools-\$(uname -r)."
    exit 1
fi

if [[ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)" -gt 1 ]]; then
    echo "[perf] WARNING: kernel.perf_event_paranoid > 1, perf may fail."
    echo "  Run: echo 0 | sudo tee /proc/sys/kernel/perf_event_paranoid"
fi

ts="$(date +%Y%m%d_%H%M%S)"
out_dir="$RESULTS_DIR/perf_${ts}"
mkdir -p "$out_dir"
echo "[perf] output dir: $out_dir"

kill_backends() {
    $PSQL -tAc \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type='client backend';" >/dev/null 2>&1 || true
}

# --- 1. perf stat: cycles/instructions per query, 3 mode × 3 run ---
stat_csv="$out_dir/perf_stat.csv"
echo "mode,run,iters,cycles,instructions,branches,branch_misses,cache_misses,task_clock_us" > "$stat_csv"

run_perf_stat() {
    local mode="$1"
    local pgopts="${2:-}"
    local r out

    echo "[perf] stat mode=$mode iters=$ITERS_STAT"
    for r in 1 2 3; do
        # perf stat без sudo (perf_event_paranoid <= 1 это позволяет).
        # PGOPTIONS прокидываем через env перед командой.
        out="$(
            env "PGOPTIONS=$pgopts" perf stat -x ',' \
                -e cycles,instructions,branches,branch-misses,cache-misses,task-clock \
                -- "$PG_BIN/psql" -p "$PG_PORT" -d "$PG_DB" -q \
                    -c "SET bench.iters TO $ITERS_STAT" \
                    -f "$SCRIPT_DIR/scripts/inproc_microbench.sql" \
            2>&1
        )"
        # parse perf stat -x output
        # format: value,unit,event,..., one per line
        cy=$(echo "$out" | awk -F, '$3=="cycles"{print $1}' | head -1)
        ins=$(echo "$out" | awk -F, '$3=="instructions"{print $1}' | head -1)
        br=$(echo "$out" | awk -F, '$3=="branches"{print $1}' | head -1)
        brm=$(echo "$out" | awk -F, '$3=="branch-misses"{print $1}' | head -1)
        cm=$(echo "$out" | awk -F, '$3=="cache-misses"{print $1}' | head -1)
        tc=$(echo "$out" | awk -F, '$3=="task-clock"{print $1}' | head -1)
        echo "$mode,$r,$ITERS_STAT,$cy,$ins,$br,$brm,$cm,$tc" | tee -a "$stat_csv"
    done
}

# Mode 1
$PSQL -tAc "ALTER SYSTEM RESET session_preload_libraries;" >/dev/null
$PGCTL reload >/dev/null; kill_backends; sleep 0.3
run_perf_stat "no_preload"

# Mode 2
$PSQL -tAc "ALTER SYSTEM SET session_preload_libraries='pg_query_stack';" >/dev/null
$PGCTL reload >/dev/null; kill_backends; sleep 0.3
run_perf_stat "enabled_on"

# Mode 3
run_perf_stat "enabled_off" "-c pg_query_stack.enabled=off"

# --- 2. perf record: горячие символы во время длинного цикла, mode=enabled_on ---
echo "[perf] record mode=enabled_on duration=${PERF_DURATION}s"
$PSQL -tAc "ALTER SYSTEM SET session_preload_libraries='pg_query_stack';" >/dev/null
$PGCTL reload >/dev/null; kill_backends; sleep 0.3

# Запускаем длинный цикл в фоне
nohup $PG_BIN/psql -p "$PG_PORT" -d "$PG_DB" \
  -c "SET bench.iters TO $ITERS_PERF" \
  -f "$SCRIPT_DIR/scripts/inproc_microbench.sql" >/dev/null 2>&1 &
LOAD_PID=$!
disown
sleep 2

BPID="$(
    $PSQL -tAc "SELECT pid FROM pg_stat_activity WHERE state='active' AND query LIKE '%EXECUTE%SELECT 1%' LIMIT 1"
)"

if [[ -z "$BPID" ]]; then
    echo "[perf] ERROR: cannot find loaded backend PID"
    kill $LOAD_PID 2>/dev/null || true
    exit 1
fi

echo "[perf] sampling backend pid=$BPID"
perf record -F "$PERF_FREQ" --call-graph dwarf,4096 -p "$BPID" \
    -o "$out_dir/perf.data" -- sleep "$PERF_DURATION"

# top symbols: полный отчёт в topsymbols_full.txt, отфильтрованный — в topsymbols.txt
perf report -i "$out_dir/perf.data" --stdio --no-children -F overhead,symbol 2>/dev/null \
    > "$out_dir/topsymbols_full.txt" || true
grep -iE 'pg_query_stack|standard_Executor|sigsetjmp|__memcpy|memcpy@plt|strnlen|MemoryContextAlloc|materialize_frame' \
    "$out_dir/topsymbols_full.txt" > "$out_dir/topsymbols.txt" || true

echo "[perf] top symbols (filtered):"
cat "$out_dir/topsymbols.txt" | head -25

# --- 3. FlameGraph если установлен ---
if [[ -d "$FG_DIR" && -x "$FG_DIR/stackcollapse-perf.pl" ]]; then
    echo "[perf] generating flamegraph"
    perf script -i "$out_dir/perf.data" 2>/dev/null \
        | "$FG_DIR/stackcollapse-perf.pl" \
        | "$FG_DIR/flamegraph.pl" \
            --title "pg_query_stack v2 — enabled_on" \
            --colors java \
        > "$out_dir/flamegraph.svg"
    echo "[perf] flamegraph.svg saved"
else
    echo "[perf] FlameGraph not installed at $FG_DIR — skipping svg"
    echo "  install: git clone https://github.com/brendangregg/FlameGraph $FG_DIR"
fi

# гасим фоновый цикл
kill $LOAD_PID 2>/dev/null || true
wait 2>/dev/null || true

# --- 4. сводка ---
report="$RESULTS_DIR/report_${ts}.md"
{
    echo "# pg_query_stack benchmark report — $ts"
    echo
    echo "## perf stat (mode × cycles/instructions/branches per ${ITERS_STAT}-iter run)"
    echo
    python3 - <<EOF
import csv, statistics
rows = list(csv.DictReader(open("$stat_csv")))
modes = ["no_preload", "enabled_on", "enabled_off"]

def to_num(v):
    """Tolerant numeric parser: '<not supported>', empty → None."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def med(rs, k):
    vals = [to_num(r[k]) for r in rs]
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None

def fmt(v, w, prec):
    return f"{v:{w}.{prec}f}" if v is not None else f"{'n/a':>{w}}"

# task_clock в perf stat -x возвращается в МИКРОСЕКУНДАХ как целое число.
# На aarch64 VM (Parallels) hardware counters недоступны — выводятся <not supported>.
# Software counter task-clock работает всегда → пользуемся им для дельт.
print("| mode        | cycles/iter | instr/iter | IPC  | branch_miss/iter | cache_miss/iter | task_clock_ns/iter |")
print("|-------------|-------------|------------|------|------------------|-----------------|--------------------|")
for m in modes:
    rs = [r for r in rows if r["mode"]==m]
    if not rs: continue
    iters = int(rs[0]["iters"])
    cy = med(rs, "cycles")
    ins = med(rs, "instructions")
    ipc = (ins/cy) if (cy and ins) else None
    bm = med(rs, "branch_misses")
    cm = med(rs, "cache_misses")
    tc_us = med(rs, "task_clock_us")  # микросекунды, целое
    cy_p = (cy/iters) if cy is not None else None
    ins_p = (ins/iters) if ins is not None else None
    bm_p = (bm/iters) if bm is not None else None
    cm_p = (cm/iters) if cm is not None else None
    # В csv стоит "task_clock_us", но perf stat реально пишет туда msec×1000
    # (т.е. микросекунды без точки). Делим на iters → получаем us/iter, ×1000 = ns/iter.
    tc_p = (tc_us*1000/iters) if tc_us is not None else None
    print(f"| {m:<11} | {fmt(cy_p,11,1)} | {fmt(ins_p,10,1)} | {fmt(ipc,4,2)} | {fmt(bm_p,16,3)} | {fmt(cm_p,15,3)} | {fmt(tc_p,18,1)} |")

print()
no = [r for r in rows if r["mode"]=="no_preload"]
on = [r for r in rows if r["mode"]=="enabled_on"]
no_iters = int(no[0]["iters"]) if no else 1
on_iters = int(on[0]["iters"]) if on else 1
no_tc = med(no, "task_clock_us")
on_tc = med(on, "task_clock_us")
if no_tc is not None and on_tc is not None:
    delta_ns = ((on_tc/on_iters)-(no_tc/no_iters))*1000
    print(f"Δ task_clock (enabled_on - no_preload) = {delta_ns:.1f} ns/query")
    print(f"  (включает per-iter overhead psql DO-loop, см. run_micro.sh для чистого ns/iter)")
no_cy = med(no, "cycles")
on_cy = med(on, "cycles")
if no_cy is not None and on_cy is not None:
    print(f"Δ cycles (enabled_on - no_preload) = {(on_cy/on_iters)-(no_cy/no_iters):.1f} cycles/query")
print()
EOF

    echo "## perf top symbols (mode=enabled_on)"
    echo
    echo '```'
    cat "$out_dir/topsymbols.txt"
    echo '```'
    echo
    if [[ -f "$out_dir/flamegraph.svg" ]]; then
        echo "## FlameGraph"
        echo
        echo "[flamegraph.svg](perf_${ts}/flamegraph.svg) — открыть в браузере"
    fi
} > "$report"

echo "[perf] report: $report"
