-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_query_stack UPDATE TO '1.1.5'" to load this file. \quit

-- Version 1.1.5: Hot-path micro-optimizations
--   * Pre-baked chained_ExecutorStart / chained_ExecutorEnd pointers (item 1.1.5/A)
--   * Single-pass memccpy in materialize_frame inline branch (item 1.1.5/B)
-- No schema changes required - all improvements are in the C code.
-- This is a no-op migration for users upgrading from 1.1.4.
