-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_query_stack" to load this file. \quit

-- Возвращает таблицу полного стека SQL-запросов текущей сессии.
-- frame_number = 0 — top-level запрос, N — самый вложенный.
-- _skip_count (по умолчанию 1) — сколько фреймов пропустить от вершины
-- (типично: 1 пропускает сам вызов pg_query_stack()).
CREATE FUNCTION public.pg_query_stack(_skip_count int DEFAULT 1)
	RETURNS TABLE (frame_number integer, query_text text)
	AS 'MODULE_PATHNAME'
	LANGUAGE C VOLATILE;

DROP EXTENSION IF EXISTS pg_self_query;

CREATE OR REPLACE FUNCTION public.pg_self_query ()
    RETURNS table
            (
                frame_number integer,
                query_text   text
            )
AS
$$
SELECT
    frame_number,
    query_text
FROM public.pg_query_stack(2)
$$ LANGUAGE sql;
