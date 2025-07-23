-- Тест 006: Вложенные вызовы с параметром 0 (полный стек)
CREATE OR REPLACE FUNCTION test_nested_stack2 ()
    RETURNS table
            (
                frame_number int,
                query_text   text
            )
AS
$$
BEGIN
    RETURN QUERY SELECT
                     *
                 FROM pg_query_stack(0);
END;
$$ LANGUAGE plpgsql;

-- Вызываем функцию с вложенным стеком
SELECT
    *
FROM test_nested_stack2();

-- Удаляем тестовую функцию
DROP FUNCTION test_nested_stack2();
