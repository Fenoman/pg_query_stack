#!/usr/bin/env bash
# Бенчмарк hot-path (microseconds per query) для PG 16/17/18
# Обёртка над run_micro.sh с итерацией по трём версиям PostgreSQL.
# Порты: 5416 (PG16), 5417 (PG17), 5418 (PG18)
#
# OUT_DIR=${OUT_DIR:-./results/cross_version} ./bench/run_micro_all_versions.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/results/cross_version}"
mkdir -p "$OUT_DIR"

PG_VERSIONS=(16 17 18)

for V in "${PG_VERSIONS[@]}"; do
    export PG_BIN="/home/bench/pg${V}rel/bin"
    export PG_DATA="/home/bench/pg${V}rel-bench-data"
    export PG_PORT="54${V}"   # concat -> 5416 / 5417 / 5418
    export PG_DB="${PG_DB:-postgres}"
    export RUNS="${RUNS:-7}"
    export ITERS="${ITERS:-200000}"

    echo
    echo "============================================================"
    echo " PG ${V}  bin=$PG_BIN  data=$PG_DATA  port=$PG_PORT"
    echo "============================================================"

    if [[ ! -x "$PG_BIN/pg_ctl" ]]; then
        echo "Ошибка: pg_ctl не найден в $PG_BIN" >&2
        exit 1
    fi
    if [[ ! -d "$PG_DATA" ]]; then
        echo "Ошибка: data dir $PG_DATA не найден" >&2
        exit 1
    fi
    if ! "$PG_BIN/pg_ctl" -D "$PG_DATA" status >/dev/null 2>&1; then
        echo "Запуск PG${V} сервера..."
        "$PG_BIN/pg_ctl" -D "$PG_DATA" -l "$PG_DATA/logfile" start
        sleep 1
    fi

    log="$OUT_DIR/pg${V}_micro.log"
    "$SCRIPT_DIR/run_micro.sh" 2>&1 | tee "$log"

    # Копирование последнего CSV (run_micro.sh выводит micro_<timestamp>.csv)
    last_csv="$(ls -1t "$SCRIPT_DIR/results/micro_"*.csv 2>/dev/null | head -1 || true)"
    if [[ -n "$last_csv" ]]; then
        cp "$last_csv" "$OUT_DIR/pg${V}_micro.csv"
    fi
done

# Агрегированная сводка: медиана ns/iter по версиям
{
    echo
    echo "===== Результаты по версиям (7 прогонов, медиана) ====="
    printf '%-6s %-12s %-12s %-12s %-12s\n' "PG" "no_preload" "enabled_on" "enabled_off" "delta"
    for V in "${PG_VERSIONS[@]}"; do
        csv="$OUT_DIR/pg${V}_micro.csv"
        if [[ ! -f "$csv" ]]; then
            printf '%-6s %s\n' "$V" "не найден"
            continue
        fi
        python3 - "$csv" "$V" <<'PYEOF'
import csv, statistics, sys
path, v = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(path)))
def med(mode):
    vals = [float(r["ns_per_iter"]) for r in rows if r["mode"] == mode]
    return statistics.median(vals) if vals else float("nan")
n = med("no_preload")
y = med("enabled_on")
print(f"{v:<6} {n:<12.2f} {y:<12.2f} {med('enabled_off'):<12.2f} {y-n:<+12.2f}")
PYEOF
    done
} | tee "$OUT_DIR/summary.txt"

echo
echo "Готово. Результаты в $OUT_DIR"
