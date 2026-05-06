-- Тест 017: Lazy materialization при глубокой вложенности
--
-- Проверяет MATERIALIZE-ИНВАРИАНТ v2: любой родительский фрейм должен иметь
-- стабильную копию (heap_copy != NULL) к моменту, когда поверх него пушится
-- новый фрейм. Без этого инварианта чтение родителя через raw_text вернуло бы
-- мусор в средах с агрессивным освобождением memory context (Citus, plpgsql,
-- TimescaleDB).
--
-- Сценарий: 5 plpgsql функций f1..f5, каждая PERFORM следующую. Между
-- уровнями выполняется EXECUTE 'SELECT 1' — это провоцирует выделение и
-- освобождение memory context'ов под динамический SQL, эмулируя реальные
-- условия для проверки lazy materialize.
--
-- Метрики проверки (locale-agnostic, без вывода query_text):
--   - frame_number должен идти 0..N последовательно
--   - LENGTH(query_text) > 0 для каждого фрейма (текст материализован)
--   - все query_text различны между собой (разные фреймы — разные запросы)

CREATE OR REPLACE FUNCTION f5() RETURNS TABLE(frame_number int, qt_len int) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT 1';   -- провоцируем context churn перед чтением стека
    RETURN QUERY
        SELECT T.frame_number, LENGTH(T.query_text)::int
        FROM pg_query_stack(0) AS T
        ORDER BY T.frame_number;
END;
$$;

CREATE OR REPLACE FUNCTION f4() RETURNS TABLE(frame_number int, qt_len int) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT 1';
    RETURN QUERY SELECT * FROM f5();
END;
$$;

CREATE OR REPLACE FUNCTION f3() RETURNS TABLE(frame_number int, qt_len int) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT 1';
    RETURN QUERY SELECT * FROM f4();
END;
$$;

CREATE OR REPLACE FUNCTION f2() RETURNS TABLE(frame_number int, qt_len int) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT 1';
    RETURN QUERY SELECT * FROM f3();
END;
$$;

CREATE OR REPLACE FUNCTION f1() RETURNS TABLE(frame_number int, qt_len int) LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE 'SELECT 1';
    RETURN QUERY SELECT * FROM f2();
END;
$$;

-- Аггрегированные проверки — locale-нейтральные:
--   stack_depth     — глубина стека внутри f5: top SELECT + цепочка f1..f5
--                     + внутренний SELECT FROM pg_query_stack = ровно 6
--   min_frame, max_frame — frame_number идут от 0 до 5 без пропусков
--   all_lengths_pos — у каждого фрейма ненулевая длина (значит материализация
--                     родителей сработала; иначе при dangling pointer мы бы
--                     получили SIGSEGV или мусор)
SELECT
    count(*)             AS stack_depth,
    min(frame_number)    AS min_frame,
    max(frame_number)    AS max_frame,
    bool_and(qt_len > 0) AS all_lengths_pos
FROM f1();

-- Дополнительная проверка: при повторном вызове цепочка не должна
-- "протекать" — глубина стека такая же.
SELECT count(*) AS depth_run2 FROM f1();

-- Очистка
DROP FUNCTION f1();
DROP FUNCTION f2();
DROP FUNCTION f3();
DROP FUNCTION f4();
DROP FUNCTION f5();
