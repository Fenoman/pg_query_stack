-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_query_stack UPDATE TO '2.0.0'" to load this file. \quit

-- Миграция 1.1.5 → 2.0.0: no-op.
-- Публичный SQL API не изменился — пользовательский код работает без модификаций.
