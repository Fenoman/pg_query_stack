# contrib/pg_query_stack/Makefile

MODULES = pg_query_stack
EXTENSION = pg_query_stack
EXTVERSION = 1.0.5
DATA = $(EXTENSION)--$(EXTVERSION).sql
PGFILEDESC = "pg_query_stack - tool to get full query stack of current backend"
CONTROL = pg_query_stack.control
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)