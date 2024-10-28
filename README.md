## About the Extension

The `pg_query_stack` extension allows you to retrieve the full stack of SQL queries for the current session (unlike the `current_query()` function, which returns only the top-level query). This is useful for debugging, logging, and analyzing complex call chains in the database.

It works based on custom hooks. The source code is extensively commented (currently only in Russian).

Unlike the previous version of the extension (`pg_self_query`), the current one has been rewritten from scratch and does not depend on the `pg_query_state` module or PostgreSQL core patches. Therefore, it can be installed independently on a vanilla version of PostgreSQL and likely on any fork.

## Features

- Retrieve the full stack of nested SQL queries for the current session.
- Independence from other modules (does not depend on `pg_query_state` like the previous version).
- Compatible with vanilla PostgreSQL and likely with any forks.
- Extensively commented source code (currently only in Russian).

## Compatibility

Currently, the extension is compatible with PostgreSQL 16. Supporting earlier versions may require minor modifications to the source code. Compatibility with future versions of PostgreSQL is not guaranteed and needs to be tested.

## Use Cases

This module can be useful in the following situations:

- Logging (via triggers) of queries that modified data in a table.
- Capturing the actual result of a DSQL query that was executed and recording it somewhere.
- Debugging very complex functions or triggers with intricate call chains.

## Installation

1. After cloning this repository and navigating to the extension's folder, run:

    ```bash
    make install USE_PGXS=1
    ```

    Make sure that `pg_config` points to the desired version of PostgreSQL if you have multiple versions installed!

2. Then, change the `session_preload_libraries` parameter in `postgresql.conf`:

    ```
    session_preload_libraries = 'pg_query_stack'
    ```

    The extension should be loaded into the session! Do not add it to `shared_preload_libraries`.

3. Restart PostgreSQL:

    ```bash
    sudo systemctl restart postgresql  # or another method to restart
    ```

4. In the target database, run to create the extension:

    ```sql
    CREATE EXTENSION pg_query_stack;
    ```

5. Done!

## Description of the `pg_query_stack` Function

```sql
pg_query_stack(_skip_count int DEFAULT 1)
    RETURNS TABLE (
        frame_number integer,
        query_text text
    )
```

As a result of executing the function, a table containing the query stack will be returned, starting from the top-level query (frame number 0) down to the lowest level (frame number N) minus the skipped frames.

What the `_skip_count` parameter means:

- `0` — returns the entire query stack "as is," including the query where `pg_query_stack` itself is called.
- `1` — (default) returns the stack without the query where `pg_query_stack` itself is called.
- `N` — the specified number of queries in the stack starting from the lowest level will be skipped.

## Example of the Extension's Operation

Let's create two functions in the database:

```sql
CREATE OR REPLACE FUNCTION test() RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN
   DROP TABLE IF EXISTS test1;
   CREATE TEMP TABLE test1 AS
   SELECT
      *
   FROM pg_query_stack();

   DROP TABLE IF EXISTS test2;
   CREATE TEMP TABLE test2 AS
   SELECT
      *
   FROM pg_query_stack(0);
END;
$$;

CREATE OR REPLACE FUNCTION test_up() RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN
    PERFORM test();
END;
$$;
```

Now let's call the `test_up` function:

```sql
SELECT test_up();
```

Now let's see what was recorded in the `test1` table:

```sql
SELECT * FROM test1;
```

Result:

| frame_number | query_text        |
|--------------|-------------------|
| 1            | SELECT test()     |
| 0            | SELECT test_up()  |

The `test1` table contains the query stack without the current call to `pg_query_stack`, since the `_skip_count` parameter has the default value of `1`.

Now let's see what was recorded in the `test2` table:

```sql
SELECT * FROM test2;
```

Result:

| frame_number | query_text                                                 |
|--------------|------------------------------------------------------------|
| 2            | CREATE TEMP TABLE test2 AS SELECT * FROM pg_query_stack(0) |
| 1            | SELECT test()                                              |
| 0            | SELECT test_up()                                           |

The `test2` table contains the full query stack, including the current call to `pg_query_stack`, because we passed the parameter `_skip_count = 0`.

**Explanation:**

In the first case, we get the stack without the current `pg_query_stack()` query, which can be useful for obtaining information about external queries. In the second case, we see the full stack, including the innermost call, which allows us to fully trace the call chain.

## Updating the Extension Version

After compiling from the source files, execute:

```sql
DROP EXTENSION pg_query_stack;
CREATE EXTENSION pg_query_stack;
```

Then restart PostgreSQL.

## Migration from the `pg_self_query` Extension

If you have previously used the `pg_self_query` extension with the function of the same name, to ensure backward compatibility and avoid the need to change code, you can create a "wrapper":

```sql
CREATE OR REPLACE FUNCTION public.pg_self_query()
   RETURNS TABLE (
      frame_number integer,
      query_text   text
   )
AS
$$
   SELECT
      frame_number,
      query_text
   FROM public.pg_query_stack(2)
$$
LANGUAGE SQL;
```
At the moment, it is automatically created when the extension is installed.

## License

This project is licensed under the MIT License. This means you are free to use, modify, and distribute this code for commercial and non-commercial purposes, provided you retain the copyright notice.

The full text of the license is available in the [LICENSE](LICENSE.md) file.