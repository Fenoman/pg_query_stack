-- Тест 008: Работа с подтранзакциями (savepoints)
-- Этот тест проверяет корректность очистки стека при откатах подтранзакций
CREATE OR REPLACE FUNCTION public.test_subtransactions () RETURNS void
AS
$$
DECLARE
    v_result integer;
    _stack   text;
BEGIN
    RAISE INFO 'Начало основной транзакции';

    SELECT
        STRING_AGG(T.frame_number || T.query_text, ' | ')
    INTO _stack
    FROM pg_query_stack() AS T;
    RAISE INFO '%', _stack;

    -- Первое выполнение запроса
    EXECUTE 'SELECT 1' INTO v_result;
    RAISE INFO 'Первый запрос выполнен, результат: %', v_result;

    -- Начало под-транзакции
    BEGIN
        RAISE INFO 'Начало под-транзакции 1';

        -- Выполнение запроса внутри под-транзакции
        EXECUTE 'SELECT 2' INTO v_result;
        RAISE INFO 'Внутри под-транзакции 1, результат: %', v_result;

        SELECT
            STRING_AGG(T.frame_number || T.query_text, ' | ')
        INTO _stack
        FROM pg_query_stack() AS T;
        RAISE INFO '%', _stack;

        -- Генерация исключения для отката под-транзакции
        RAISE EXCEPTION 'Исключение внутри под-транзакции 1';

    EXCEPTION
        WHEN OTHERS THEN RAISE INFO 'Откат под-транзакции 1';
        SELECT
            STRING_AGG(T.frame_number || T.query_text, ' | ')
        INTO _stack
        FROM pg_query_stack() AS T;
        RAISE INFO '%', _stack;
        -- Здесь под-транзакция автоматически откатится
    END;

    RAISE INFO 'После под-транзакции 1';
    SELECT
        STRING_AGG(T.frame_number || T.query_text, ' | ')
    INTO _stack
    FROM pg_query_stack() AS T;
    RAISE INFO '%', _stack;

    -- Начало под-транзакции без исключения
    BEGIN
        RAISE INFO 'Начало под-транзакции 2';

        -- Выполнение запроса внутри под-транзакции
        EXECUTE 'SELECT 3' INTO v_result;
        RAISE INFO 'Внутри под-транзакции 2, результат: %', v_result;

        -- Под-транзакция успешно завершается
        SELECT
            STRING_AGG(T.frame_number || T.query_text, ' | ')
        INTO _stack
        FROM pg_query_stack() AS T;
        RAISE INFO '%', _stack;
    END;

    RAISE INFO 'После под-транзакции 2';

    -- Второе выполнение запроса
    EXECUTE 'SELECT 4' INTO v_result;
    RAISE INFO 'Второй запрос выполнен, результат: %', v_result;

    RAISE INFO 'Конец основной транзакции';
    SELECT
        STRING_AGG(T.frame_number || T.query_text, ' | ')
    INTO _stack
    FROM pg_query_stack() AS T;
    RAISE INFO '%', _stack;
END;
$$ LANGUAGE plpgsql;

SELECT
    public.test_subtransactions();

DROP FUNCTION public.test_subtransactions;
