-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_query_stack UPDATE TO '1.1.5'" to load this file. \quit

-- Миграция 1.1.4 → 1.1.5: схема не меняется, изменения только в C-коде.
