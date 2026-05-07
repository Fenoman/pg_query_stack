#!/bin/bash
# Сборка, установка и тестирование расширения pg_query_stack
set -e

echo "=== Сборка расширения pg_query_stack ==="
make clean
make

echo ""
echo "=== Установка расширения ==="
sudo make install

echo ""
echo "=== Запуск регрессионных тестов ==="
make installcheck

echo ""
echo "=== Тесты выполнены успешно! ==="

# При наличии различий в выводе
if [ -f regression.diffs ]; then
    echo ""
    echo "=== Найдены различия в тестах ==="
    cat regression.diffs
    exit 1
fi

# Сводка результатов
echo ""
echo "=== Результаты тестов ==="
test_files=(001_setup 002_basic_functionality 003_skip_parameters 004_edge_cases 005_nested_calls_default 006_nested_calls_full 007_error_recovery 008_subtransactions 009_complex_nesting 010_loop 011_triggers 012_stack_overflow 013_query_length_limit 014_cte_recursive 015_after_trigger_error_cleanup 016_swallowed_subxact_trigger_error 017_lazy_materialize_deep 018_subxact_only_cleanup 019_chained_extensions 020_audit_trigger_realistic 022_columnar_basic 023_audit_after_update_pg_self_query 024_audit_after_delete_pg_self_query 025_audit_nested_function_deepest_visible)

for test in "${test_files[@]}"; do
    if [ -f "results/${test}.out" ]; then
        echo "✓ ${test}: PASSED"
    else
        echo "✗ ${test}: РЕЗУЛЬТАТ НЕ НАЙДЕН"
    fi
done

echo ""
echo "=== Отдельные тесты ==="
echo "Детальный вывод теста: cat results/[имя_теста].out"
echo "Доступные результаты:"
ls -la results/ 2>/dev/null || echo "Папка results/ не найдена"
