-- Тест 023: AFTER UPDATE STATEMENT + REFERENCING + pg_self_query()
--
-- Воспроизводит точный паттерн audit.trigger_audit_data из OmniX_DB:
-- AFTER UPDATE STATEMENT триггер с переходными таблицами,
-- захватывающий top-level UPDATE через pg_self_query() ORDER BY frame_number DESC LIMIT 1.
--
-- Главный инвариант: pg_self_query() (= pg_query_stack(2)) скрывает
-- внутренние слои стека (SQL-обёртку и SPI-вызов изнутри триггера),
-- а самый глубокий visible frame — это top-level UPDATE.
--
-- Тест проверяет:
--   1. Триггер срабатывает на AFTER UPDATE STATEMENT
--   2. Захваченный текст начинается с UPDATE и содержит имя таблицы
--   3. captured_frame = 0 (top-level UPDATE)
--   4. Захвачен ровно один фрейм на одно срабатывание триггера

DROP TABLE IF EXISTS t023_data CASCADE;
DROP TABLE IF EXISTS t023_log CASCADE;

CREATE TABLE t023_data
(
    link int PRIMARY KEY,
    val  text
);

CREATE TABLE t023_log
(
    captured_query text,
    captured_frame int,
    captured_op    text
);

INSERT INTO t023_data VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Точная копия паттерна audit.trigger_audit_data
CREATE OR REPLACE FUNCTION t023_capture() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    _query_text text;
    _frame      int;
BEGIN
    SELECT X.query_text, X.frame_number
    INTO _query_text, _frame
    FROM public.pg_self_query() X
    ORDER BY X.frame_number DESC
    LIMIT 1;

    INSERT INTO t023_log VALUES (_query_text, _frame, TG_OP);
    RETURN NULL;
END;
$$;

-- AFTER UPDATE STATEMENT с переходными таблицами (как в OmniX)
CREATE TRIGGER t023_after_update
    AFTER UPDATE ON t023_data
    REFERENCING OLD TABLE AS deleted NEW TABLE AS inserted
    FOR EACH STATEMENT
    EXECUTE FUNCTION t023_capture();

-- ============================================================
-- Сценарий: top-level UPDATE
-- ============================================================
UPDATE t023_data SET val = val || '_upd';

SELECT
    captured_op,
    captured_frame,
    captured_query LIKE 'UPDATE%'    AS starts_with_update,
    captured_query LIKE '%t023_data%' AS contains_table_name,
    count(*) OVER ()                  AS rows_in_log
FROM t023_log;

-- Cleanup
DROP TRIGGER t023_after_update ON t023_data;
DROP FUNCTION t023_capture();
DROP TABLE t023_data;
DROP TABLE t023_log;
