#!/usr/bin/env bash
# bench/setup.sh — поднимает чистый PostgreSQL-инстанс под бенчи.
# Идемпотентен: если data dir уже существует и postgres запущен — пропускает.
set -euo pipefail

PG_BIN="${PG_BIN:-/home/bench/pg16rel/bin}"
PG_DATA="${PG_DATA:-/home/bench/pg16rel-bench-data}"
PG_PORT="${PG_PORT:-55444}"
PG_DB="${PG_DB:-postgres}"

PSQL="$PG_BIN/psql"
PGCTL="$PG_BIN/pg_ctl"
INITDB="$PG_BIN/initdb"

log() { echo "[setup] $*"; }

# 1. initdb если data dir отсутствует
if [[ ! -f "$PG_DATA/PG_VERSION" ]]; then
    log "initdb $PG_DATA (locale=C.UTF-8, encoding=UTF8)"
    "$INITDB" -D "$PG_DATA" --locale=C.UTF-8 --encoding=UTF8 --no-sync >/dev/null

    cat >> "$PG_DATA/postgresql.conf" <<EOF

# bench tuning (added by bench/setup.sh)
port = $PG_PORT
shared_buffers = 256MB
fsync = off
synchronous_commit = off
full_page_writes = off
checkpoint_timeout = 1h
max_wal_size = 4GB
max_locks_per_transaction = 256
log_min_messages = warning
log_statement = 'none'
EOF
else
    log "data dir $PG_DATA already initialised; reusing"
fi

# 2. start если не запущен
if ! "$PGCTL" -D "$PG_DATA" status >/dev/null 2>&1; then
    log "starting postgres on port $PG_PORT"
    "$PGCTL" -D "$PG_DATA" -l "$PG_DATA/logfile" start >/dev/null
    sleep 1
else
    log "postgres already running"
fi

# 3. устанавливаем CREATE EXTENSION
"$PSQL" -p "$PG_PORT" -d "$PG_DB" -tAc "CREATE EXTENSION IF NOT EXISTS pg_query_stack;" >/dev/null
log "extension pg_query_stack: $("$PSQL" -p "$PG_PORT" -d "$PG_DB" -tAc 'SELECT extversion FROM pg_extension WHERE extname=$$pg_query_stack$$')"

# 4. session_preload по умолчанию ВЫКЛЮЧЕН: его включают/выключают сами бенчи
"$PSQL" -p "$PG_PORT" -d "$PG_DB" -tAc "ALTER SYSTEM RESET session_preload_libraries;" >/dev/null
"$PGCTL" -D "$PG_DATA" reload >/dev/null
log "session_preload_libraries reset; ready for bench scripts"

cat <<EOF

bench cluster ready:
  PG_BIN  = $PG_BIN
  PG_DATA = $PG_DATA
  PG_PORT = $PG_PORT
  PG_DB   = $PG_DB

next: ./run_micro.sh, ./run_pgbench.sh, ./run_perf.sh, или ./run_all.sh
EOF
