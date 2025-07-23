-- Тест 004: Граничные случаи и валидация параметров
-- Проверим большие значения skip_count
SELECT COUNT(*) as row_count_1000 FROM pg_query_stack(1000);
SELECT COUNT(*) as row_count_100000 FROM pg_query_stack(100000);

-- Проверим отрицательные значения (должны трактоваться как 0)
SELECT frame_number, 
       CASE 
           WHEN query_text LIKE '%pg_query_stack(-1)%' THEN 'Contains pg_query_stack(-1) call'
           ELSE 'Unexpected query: ' || query_text 
       END as test_result
FROM pg_query_stack(-1);