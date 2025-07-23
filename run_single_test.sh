#!/bin/bash

# Скрипт для запуска отдельного теста pg_query_stack

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
    echo "  010_loop                  - Нагрузочный тест с циклом 1000 итераций"
    echo "  011_triggers              - Работа с триггерами"
    echo "  012_stack_overflow        - Переполнение стека"
    echo "  013_query_length_limit    - Ограничение длины запроса"
    echo "  014_cte_recursive         - CTE и рекурсивные запросы"
    echo ""
    echo "Примеры:"
    echo "  $0 001_setup"
    echo "  $0 007_error_recovery"
    exit 1
fi

TEST_NAME=$1

# Проверяем, существует ли тест
if [ ! -f "sql/${TEST_NAME}.sql" ]; then
    echo "Ошибка: Тест ${TEST_NAME} не найден!"
    echo "Файл sql/${TEST_NAME}.sql не существует."
    exit 1
fi

echo "=== Запуск теста: ${TEST_NAME} ==="
echo ""

# Запускаем конкретный тест
make installcheck REGRESS="${TEST_NAME}"

echo ""
if [ -f "regression.diffs" ]; then
    echo "=== ТЕСТ НЕ ПРОШЕЛ - Различия найдены ==="
    cat regression.diffs
    exit 1
else
    echo "=== ТЕСТ ПРОШЕЛ УСПЕШНО ==="
fi

# Показываем результат, если он есть
if [ -f "results/${TEST_NAME}.out" ]; then
    echo ""
    echo "=== Результат теста ==="
    echo "Файл: results/${TEST_NAME}.out"
    echo "Размер: $(wc -l < results/${TEST_NAME}.out) строк"
    echo ""
    echo "Для просмотра полного результата выполните:"
    echo "cat results/${TEST_NAME}.out"
fi