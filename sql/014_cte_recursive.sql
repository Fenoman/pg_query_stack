-- Тест 014: CTE и рекурсивные запросы
-- Этот тест проверяет корректность работы с Common Table Expressions
-- включая рекурсивные CTE и вложенные CTE

-- Рекурсивный CTE тест 1
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
                                                        -- Базовый случай
                                                        SELECT
                                                            1,
                                                            0::bigint,
                                                            1::bigint
                                                        UNION ALL
                                                        -- Рекурсивный случай
                                                        SELECT
                                                            n + 1,
                                                            fib_n_plus_1,
                                                            fib_n + fib_n_plus_1
                                                        FROM fibonacci
                                                        WHERE n < 10
                                                    )
SELECT
    MAX(fib_n) AS max_fibonacci,
    (
        SELECT
            COUNT(*)
        FROM pg_query_stack(0)
    ) AS recursive_stack_entries
FROM fibonacci;


-- Рекурсивный CTE тест 2
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
                                                        -- Базовый случай
                                                        SELECT
                                                            1,
                                                            0::bigint,
                                                            1::bigint
                                                        UNION ALL
                                                        -- Рекурсивный случай
                                                        SELECT
                                                            n + 1,
                                                            fib_n_plus_1,
                                                            fib_n + fib_n_plus_1
                                                        FROM fibonacci
                                                        WHERE n < 10
                                                    )
SELECT
    fib_n AS fibonacci,
    (
        SELECT
            COUNT(*)
        FROM pg_query_stack(0)
    ) AS recursive_stack_entries
FROM fibonacci;


-- Рекурсивный CTE тест 3
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
                                                        -- Базовый случай
                                                        SELECT
                                                            1,
                                                            0::bigint,
                                                            1::bigint,
                                                            T.*
                                                        FROM pg_query_stack(0) AS T
                                                        UNION ALL
                                                        -- Рекурсивный случай
                                                        SELECT
                                                            n + 1,
                                                            fib_n_plus_1,
                                                            fib_n + fib_n_plus_1,
                                                            T.*
                                                        FROM fibonacci, pg_query_stack(0) AS T
                                                        WHERE n < 10
                                                    )
SELECT
    *
FROM fibonacci;


-- Вложенные CTE
CREATE OR REPLACE FUNCTION test_nested_cte ()
    RETURNS table
            (
                outer_value int,
                frame_number_in int,
                query_text_in text,
                frame_number int,
                query_text text
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY WITH outer_cte AS (
                                       SELECT
                                           GENERATE_SERIES(1, 3) AS outer_id
                                   ),
                      inner_cte AS (
                                       SELECT
                                           o.outer_id,
                                           T.*
                                       FROM outer_cte o, pg_query_stack(0) AS T
                                    )
                 SELECT
                     ic.outer_id::int,
                     ic.frame_number AS  frame_number_in,
                     ic.query_text AS query_text_in,
                     T.*
                 FROM inner_cte ic, pg_query_stack(0) AS T
                 LIMIT 1;
END;
$$;

SELECT
    *
FROM test_nested_cte();

-- Очистка
DROP FUNCTION test_nested_cte();

DISCARD ALL;