-- Тест 001: Базовая установка и проверка расширения
-- Загружаем расширение
CREATE EXTENSION IF NOT EXISTS pg_query_stack;

-- Проверяем, что функция существует
\df pg_query_stack

-- Проверяем, что функция pg_self_query существует  
\df pg_self_query

-- Базовая проверка работоспособности
SELECT COUNT(*) >= 0 as basic_check FROM pg_query_stack();