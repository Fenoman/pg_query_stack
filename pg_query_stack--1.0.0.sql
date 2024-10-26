-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_query_stack" to load this file. \quit

CREATE FUNCTION pg_query_stack(_skip_count int DEFAULT 1)
	RETURNS TABLE (frame_number integer, query_text text)
	AS 'MODULE_PATHNAME'
	LANGUAGE C VOLATILE;