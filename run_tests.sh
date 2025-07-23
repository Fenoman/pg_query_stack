#!/bin/bash

# Скрипт для сборки и тестирования расширения pg_query_stack

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

# Если тесты не прошли, показываем различия
if [ -f regression.diffs ]; then
    echo ""
    echo "=== Найдены различия в тестах ==="
    cat regression.diffs
    exit 1
fi

# Показываем краткую сводку результатов
echo ""
echo "=== Сводка результатов тестов ==="
test_files=(001_setup 002_basic_functionality 003_skip_parameters 004_edge_cases 005_nested_calls_default 006_nested_calls_full 007_error_recovery 008_subtransactions 009_complex_nesting 010_loop 011_triggers 012_stack_overflow 013_query_length_limit 014_cte_recursive)

for test in "${test_files[@]}"; do
    if [ -f "results/${test}.out" ]; then
        echo "✓ ${test}: PASSED"
    else
        echo "✗ ${test}: РЕЗУЛЬТАТ НЕ НАЙДЕН"
    fi
done

echo ""
echo "=== Подробные результаты отдельных тестов (при необходимости) ==="
echo "Для просмотра детального вывода конкретного теста используйте:"
echo "cat results/[имя_теста].out"
echo ""
echo "Доступные файлы результатов:"
ls -la results/ 2>/dev/null || echo "Папка results/ не найдена"