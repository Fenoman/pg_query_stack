# contrib/pg_query_stack/Makefile

MODULES = pg_query_stack
EXTENSION = pg_query_stack
EXTVERSION = 1.0.6
DATA = $(EXTENSION)--$(EXTVERSION).sql
PGFILEDESC = "pg_query_stack - tool to get full query stack of current backend"
CONTROL = pg_query_stack.control

# Regression tests
REGRESS = 001_setup 002_basic_functionality 003_skip_parameters 004_edge_cases 005_nested_calls_default 006_nested_calls_full 007_error_recovery 008_subtransactions 009_complex_nesting 010_loop 011_triggers 012_stack_overflow 013_query_length_limit 014_cte_recursive
REGRESS_OPTS = --inputdir=. --outputdir=. --dbname=regression_pg_query_stack

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)