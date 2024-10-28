-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_query_stack" to load this file. \quit

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