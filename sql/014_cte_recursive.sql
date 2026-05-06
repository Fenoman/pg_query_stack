-- Тест 014: CTE и рекурсивные запросы
-- Этот тест проверяет корректность работы с Common Table Expressions
-- включая рекурсивные CTE и вложенные CTE.
--
-- LOCALE-AGNOSTIC NOTE: query_text НЕ выводится напрямую, потому что
-- сами SQL-тексты содержат русские комментарии ("-- Базовый случай")
-- и при печати столбца psql считает ширину в БАЙТАХ под LC_ALL=C
-- и в СИМВОЛАХ под UTF-8. Вместо вывода query_text проверяем его
-- через LENGTH() и LIKE — оба возвращают locale-нейтральные значения.

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


-- Рекурсивный CTE тест 3 (locale-agnostic: вместо вывода query_text
-- проверяем его длину и наличие маркера)
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1, frame_number, qt_len, qt_has_marker) AS (
                                                        -- Базовый случай
                                                        SELECT
                                                            1,
                                                            0::bigint,
                                                            1::bigint,
                                                            T.frame_number,
                                                            LENGTH(T.query_text),
                                                            T.query_text LIKE '%RECURSIVE fibonacci%'
                                                        FROM pg_query_stack(0) AS T
                                                        UNION ALL
                                                        -- Рекурсивный случай
                                                        SELECT
                                                            n + 1,
                                                            fib_n_plus_1,
                                                            fib_n + fib_n_plus_1,
                                                            T.frame_number,
                                                            LENGTH(T.query_text),
                                                            T.query_text LIKE '%RECURSIVE fibonacci%'
                                                        FROM fibonacci, pg_query_stack(0) AS T
                                                        WHERE n < 10
                                                    )
SELECT
    n,
    fib_n,
    fib_n_plus_1,
    frame_number,
    qt_len > 0 AS qt_nonempty,
    qt_has_marker
FROM fibonacci;


-- Вложенные CTE (locale-agnostic — query_text не печатаем, только длины и
-- булевы флаги наличия ожидаемых подстрок)
CREATE OR REPLACE FUNCTION test_nested_cte ()
    RETURNS table
            (
                outer_value int,
                frame_number_in int,
                qt_in_len int,
                qt_in_has_marker boolean,
                frame_number int,
                qt_len int,
                qt_has_marker boolean
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
                     ic.frame_number AS frame_number_in,
                     LENGTH(ic.query_text) AS qt_in_len,
                     ic.query_text LIKE '%test_nested_cte%' AS qt_in_has_marker,
                     T.frame_number,
                     LENGTH(T.query_text) AS qt_len,
                     T.query_text LIKE '%test_nested_cte%' AS qt_has_marker
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