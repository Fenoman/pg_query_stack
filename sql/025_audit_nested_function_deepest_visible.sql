-- Тест 025: UPDATE внутри plpgsql функции — захват самого UPDATE,
-- а не вызывающего SELECT
--
-- Главный инвариант OmniX-аудита: триггеру важно захватить SQL,
-- который непосредственно изменил данные (UPDATE), а не обёртку
-- (SELECT some_function()).
--
-- Стек выполнения в этом сценарии:
--   frame 0: SELECT t025_do_update()         -- top-level вызов
--   frame 1: UPDATE t025_data ...             -- из тела функции через SPI
--   frame 2: SELECT pg_self_query() ...       -- из тела триггера через SPI
--   frame 3: pg_query_stack(2)                -- внутри SQL-обёртки pg_self_query
--
-- pg_self_query() = pg_query_stack(2) скрывает 2 нижних слоя.
-- ORDER BY frame_number DESC LIMIT 1 даёт frame 1 = UPDATE.
--
-- Тест проверяет:
--   1. Захвачен UPDATE, а не SELECT-вызов функции
--   2. captured_frame = 1 (один уровень внутри функции)
--   3. Это поведение и в v1.1.5 и в v2.0.0 — production-инвариант

DROP TABLE IF EXISTS t025_data CASCADE;
DROP TABLE IF EXISTS t025_log CASCADE;

CREATE TABLE t025_data
(
    link int PRIMARY KEY,
    val  text
);

CREATE TABLE t025_log
(
    captured_query text,
    captured_frame int,
    captured_op    text
);

INSERT INTO t025_data VALUES (1, 'a'), (2, 'b');

CREATE OR REPLACE FUNCTION t025_capture() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    _query_text text;
    _frame      int;
BEGIN
    SELECT X.query_text, X.frame_number
    INTO _query_text, _frame
    FROM public.pg_self_query() X
    ORDER BY X.frame_number DESC
    LIMIT 1;

    INSERT INTO t025_log VALUES (_query_text, _frame, TG_OP);
    RETURN NULL;
END;
$$;

CREATE TRIGGER t025_after_update
    AFTER UPDATE ON t025_data
    REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION t025_capture();

-- Plpgsql-обёртка, выполняющая UPDATE внутри
CREATE OR REPLACE FUNCTION t025_do_update() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE t025_data SET val = val || '_x';
END;
$$;

-- ============================================================
-- Сценарий: top-level SELECT обёртки → внутри неё UPDATE → триггер
-- ============================================================
SELECT t025_do_update();

SELECT
    captured_op,
    captured_frame,
    captured_query LIKE 'UPDATE%'         AS starts_with_update,
    captured_query LIKE '%t025_data%'      AS contains_table_name,
    captured_query NOT LIKE '%t025_do_update%' AS does_not_capture_outer_select
FROM t025_log;

-- Cleanup
DROP TRIGGER t025_after_update ON t025_data;
DROP FUNCTION t025_do_update();
DROP FUNCTION t025_capture();
DROP TABLE t025_data;
DROP TABLE t025_log;
