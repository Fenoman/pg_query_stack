#!/usr/bin/env bash
# bench/run_all.sh — комплексный прогон: setup → micro → pgbench → perf →
# единый markdown-отчёт.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

ts="$(date +%Y%m%d_%H%M%S)"
log="$RESULTS_DIR/run_all_${ts}.log"

echo "[run_all] log: $log"
exec > >(tee "$log") 2>&1

echo "=== setup ==="
"$SCRIPT_DIR/setup.sh"

echo "=== micro ==="
"$SCRIPT_DIR/run_micro.sh"

echo "=== pgbench ==="
"$SCRIPT_DIR/run_pgbench.sh"

if command -v perf >/dev/null && [[ "$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo 99)" -le 1 ]]; then
    echo "=== perf ==="
    "$SCRIPT_DIR/run_perf.sh"
else
    echo "[run_all] skip perf (not Linux or perf_event_paranoid > 1)"
fi

echo "[run_all] done. Latest results:"
ls -t "$RESULTS_DIR" | head -10
