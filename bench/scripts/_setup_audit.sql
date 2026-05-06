-- Подготовка для audit_insert.sql и nested_function.sql.
-- Идемпотентно: пере-создаёт нужные объекты.
DROP TABLE IF EXISTS bench_t_audit CASCADE;
DROP TABLE IF EXISTS bench_t_data CASCADE;
DROP FUNCTION IF EXISTS bench_capture_query() CASCADE;
DROP FUNCTION IF EXISTS bench_nested_call() CASCADE;

CREATE TABLE bench_t_audit
(
    captured_query text,
    captured_op    text
);

CREATE TABLE bench_t_data
(
    id      serial PRIMARY KEY,
    payload text
);

-- BEFORE-триггер по образцу OmniX_DB audit-подсистемы: захватывает
-- top-level SQL, который меняет данные.
CREATE FUNCTION bench_capture_query() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    top_query text;
BEGIN
    SELECT query_text INTO top_query
    FROM pg_query_stack(0)
    WHERE frame_number = 0
    LIMIT 1;
    INSERT INTO bench_t_audit(captured_query, captured_op) VALUES (top_query, TG_OP);
    RETURN NEW;
END;
$$;

CREATE TRIGGER bench_t_data_capture
    BEFORE INSERT ON bench_t_data
    FOR EACH ROW
    EXECUTE FUNCTION bench_capture_query();

-- nested_function.sql: однострочная plpgsql-функция, делающая внутренний
-- SELECT — даёт глубину стека = 2 на каждом pgbench tick.
CREATE FUNCTION bench_nested_call() RETURNS int LANGUAGE plpgsql AS $$
DECLARE r int;
BEGIN
    SELECT 1 INTO r;
    RETURN r;
END;
$$;
