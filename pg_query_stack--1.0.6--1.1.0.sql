-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_query_stack UPDATE TO '1.1.0'" to load this file. \quit

-- Version 1.1.0: Performance optimization release
-- No schema changes required - all improvements are in the C code
-- This is a no-op migration for users upgrading from 1.0.6
