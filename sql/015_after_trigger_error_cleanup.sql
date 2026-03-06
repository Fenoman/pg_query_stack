-- Тест 015: Очистка стека после ошибки в AFTER trigger
-- Ошибка возникает на этапе ExecutorFinish, но перехватывается PL/pgSQL EXCEPTION.
-- В следующем вызове pg_query_stack() не должно быть "залипшего" INSERT.
DROP TABLE IF EXISTS test_err CASCADE;
CREATE TABLE test_err
(
    id int
);

CREATE OR REPLACE FUNCTION public.test_err_trigger () RETURNS trigger
    LANGUAGE plpgsql
AS
$$
BEGIN
    RAISE EXCEPTION 'boom in after trigger';
END;
$$;

CREATE TRIGGER test_err_trigger
    AFTER INSERT
    ON test_err
    FOR EACH ROW
EXECUTE PROCEDURE public.test_err_trigger();

CREATE OR REPLACE FUNCTION public.test_after_trigger_cleanup ()
    RETURNS TABLE
            (
                frame_number integer,
                query_text   text
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    BEGIN
        INSERT INTO test_err
        VALUES
            (1);
    EXCEPTION
        WHEN OTHERS THEN RETURN QUERY
            SELECT
                s.frame_number,
                s.query_text
            FROM public.pg_query_stack(0) AS s;
    END;
END;
$$;

SELECT
    *
FROM public.test_after_trigger_cleanup();

DROP FUNCTION public.test_after_trigger_cleanup();
DROP TABLE test_err CASCADE;
