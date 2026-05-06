# pg_query_stack benchmark suite

Скрипты замера накладных расходов расширения pg_query_stack.

## Что меряется

1. **micro (DO-loop)** — pure executor + hooks. Ns/iter в трёх режимах:
   - no_preload — расширение не загружено (baseline)
   - enabled_on — загружено + GUC `pg_query_stack.enabled = on`
   - enabled_off — загружено + GUC выключен
2. **pgbench** — реалистичные сценарии (`-M prepared`):
   - sel1.sql — `SELECT 1` (минимум executor work, максимум hook share)
   - nested_function.sql — plpgsql c вложенным SELECT
   - audit_insert.sql — INSERT с BEFORE-триггером, вызывающим pg_query_stack(0)
3. **perf** — глубокий разбор:
   - perf record + report — top-N горячих символов по % CPU
   - perf stat — cycles / instructions / branches per query
   - FlameGraph — визуализация стеков

## Зависимости

- PostgreSQL 16+ build с `pg_config` доступным в PATH или передан через `PG_CONFIG`
- `perf` (Linux только; на macOS только micro и pgbench)
- (Опционально) FlameGraph: `git clone https://github.com/brendangregg/FlameGraph bench/tools/FlameGraph`
- Permission to read perf events на Linux: `echo 0 | sudo tee /proc/sys/kernel/perf_event_paranoid`

## Запуск

Полный прогон (рекомендуется на Linux VM):

```bash
cd bench
./run_all.sh
# результаты в bench/results/
```

Только микро-бенч (быстро, ~1 минута):

```bash
./setup.sh           # создаст pg_data dir и поднимет инстанс
./run_micro.sh
./teardown.sh        # опционально — оставлять или гасить
```

Только pgbench:

```bash
./run_pgbench.sh     # sel1 + nested + audit, 3 режима, 5 повторов
```

Только perf (требует Linux + perf):

```bash
./run_perf.sh        # ~2 минуты — record/report/stat/flamegraph
```

## Параметры окружения

| Переменная | Дефолт | Назначение |
|---|---|---|
| `PG_BIN`   | `/home/bench/pg16rel/bin` | bin-директория PostgreSQL |
| `PG_DATA`  | `/home/bench/pg16rel-bench-data` | data-директория |
| `PG_PORT`  | `55444` | порт |
| `PG_DB`    | `postgres` | имя БД |
| `RUNS`     | 7 (micro), 5 (pgbench) | число повторов |
| `ITERS`    | 200000 | итераций в DO-цикле / в pgbench |
| `PERF_DURATION` | 30 | секунды для `perf record` |

## Формат результатов

`bench/results/` (всё gitignored, кроме README.md и .gitignore):
- `micro_<timestamp>.csv` — `mode,run,ns_per_iter`
- `pgbench_<timestamp>.csv` — `script,mode,run,tps,latency_ms`
- `perf_<timestamp>/perf.data` — raw perf record
- `perf_<timestamp>/topsymbols.txt` — отфильтрованный perf report
- `perf_<timestamp>/perf_stat.csv` — cycles/instructions per mode
- `perf_<timestamp>/flamegraph.svg` — если FlameGraph установлен
- `report_<timestamp>.md` — консолидированная сводка

## Что считать «хорошим» результатом

- micro `enabled_on` − `no_preload` ≤ **60 ns/query** (v2 baseline)
- pgbench TPS падение ≤ **5 %** vs no_preload на realistic сценариях
- perf top-symbols: pg_query_stack_ExecutorEnd / Start ≤ **1 %** CPU каждый, без `__sigsetjmp` от наших PG_TRY (их быть не должно — мы их убрали в v2)

## Интерпретация флеймграфа

- Прямоугольники с `pg_query_stack_*` — наш код
- `standard_Executor*` рядом — собственно работа PG executor
- Если видна толстая полоса `__sigsetjmp` поверх pg_query_stack_* — возможно, регрессия с возвращением PG_TRY на горячий путь
