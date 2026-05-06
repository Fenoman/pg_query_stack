#!/usr/bin/env bash
# bench/teardown.sh — гасит bench-инстанс. Data dir НЕ удаляется (для повторных
# прогонов). Удалить вручную: rm -rf "$PG_DATA".
set -euo pipefail

PG_BIN="${PG_BIN:-/home/bench/pg16rel/bin}"
PG_DATA="${PG_DATA:-/home/bench/pg16rel-bench-data}"

if "$PG_BIN/pg_ctl" -D "$PG_DATA" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$PG_DATA" stop -m fast
    echo "[teardown] postgres stopped"
else
    echo "[teardown] postgres was not running"
fi
