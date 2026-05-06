# bench results

Содержимое этой директории gitignored — здесь хранятся артефакты прогонов.

| Файл | Источник | Содержимое |
|---|---|---|
| `micro_<ts>.csv` | run_micro.sh | mode,run,ns_per_iter |
| `pgbench_<ts>.csv` | run_pgbench.sh | script,mode,run,tps,latency_ms |
| `perf_<ts>/perf.data` | run_perf.sh | raw perf record samples |
| `perf_<ts>/topsymbols.txt` | run_perf.sh | отфильтрованный perf report |
| `perf_<ts>/perf_stat.csv` | run_perf.sh | mode,event,value,elapsed |
| `perf_<ts>/flamegraph.svg` | run_perf.sh | визуализация (если FlameGraph установлен) |
| `report_<ts>.md` | run_all.sh | консолидированная сводка |
