#!/bin/bash
# Запуск одного регрессионного теста по имени
if [ $# -eq 0 ]; then
    echo "Использование: $0 <имя_теста>"
    echo ""
    echo "Доступные тесты:"
    echo "  001_setup                  - Базовая установка и проверка"
    echo "  002_basic_functionality    - Основная функциональность"
    echo "  003_skip_parameters        - Параметры пропуска"
    echo "  004_edge_cases            - Граничные случаи"
    echo "  005_nested_calls_default  - Вложенные вызовы (с пропуском)"
    echo "  006_nested_calls_full     - Вложенные вызовы (полный стек)"
    echo "  007_error_recovery        - Восстановление после ошибок"
    echo "  008_subtransactions       - Подтранзакции"
    echo "  009_complex_nesting       - Сложные вложения и временные таблицы"
    echo "  010_loop                  - Нагрузочный тест с циклом"
    echo "  011_triggers              - Работа с триггерами"
    echo "  012_stack_overflow        - Переполнение стека"
    echo "  013_query_length_limit    - Длинные запросы"
    echo "  014_cte_recursive         - CTE и рекурсивные запросы"
    echo "  015_after_trigger_error_cleanup - Очистка после ошибки AFTER"
    echo "  016_swallowed_subxact_trigger_error - Перехваченные исключения"
    echo "  017_lazy_materialize_deep - Глубокая вложенность (5+ уровней)"
    echo "  018_subxact_only_cleanup  - Очистка субтранзакций"
    echo "  019_chained_extensions    - Совместимость с другими хуками"
    echo "  020_audit_trigger_realistic - Реалистичный audit-trigger"
    echo "  022_columnar_basic         - Совместимость с Citus columnar"
    echo "  023_audit_after_update_pg_self_query - AFTER UPDATE STATEMENT + pg_self_query"
    echo "  024_audit_after_delete_pg_self_query - AFTER DELETE STATEMENT + pg_self_query"
    echo "  025_audit_nested_function_deepest_visible - UPDATE внутри функции (deepest frame)"
    echo ""
    echo "Примеры:"
    echo "  $0 001_setup"
    echo "  $0 007_error_recovery"
    exit 1
fi

TEST_NAME=$1

if [ ! -f "sql/${TEST_NAME}.sql" ]; then
    echo "Ошибка: Тест ${TEST_NAME} не найден (sql/${TEST_NAME}.sql)"
    exit 1
fi

echo "=== Запуск: ${TEST_NAME} ==="
echo ""

make installcheck REGRESS="${TEST_NAME}"

echo ""
if [ -f "regression.diffs" ]; then
    echo "=== ОШИБКА: найдены различия ==="
    cat regression.diffs
    exit 1
else
    echo "=== ОК: тест прошел ==="
fi

if [ -f "results/${TEST_NAME}.out" ]; then
    echo ""
    echo "Результат: results/${TEST_NAME}.out ($(wc -l < results/${TEST_NAME}.out) строк)"
fi
