-- Тест 003: Проверка параметров пропуска запросов
-- Проверим, что функция возвращает пустой результат при skip_count = 1
SELECT COUNT(*) as row_count FROM pg_query_stack(1);

-- Проверим, что функция возвращает пустой результат при значении по умолчанию
SELECT COUNT(*) as row_count_default FROM pg_query_stack();