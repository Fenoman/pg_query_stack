## О расширении

Расширение `pg_query_stack` позволяет получить полный стек SQL-запросов текущего сеанса (в отличие от функции `current_query()`, которая возвращает только верхний уровень).
Это полезно для отладки, логирования и анализа сложных цепочек вызовов в базе данных.

Работает на основе минимальных пользовательских хуков (ExecutorStart/End), которые записывают сырые указатели `QueryDesc*` в per-backend кольцевой буфер. SQL-функция читает кольцо в момент вызова через snapshot-once walker с тремя UAF-guard'ами. Исходный код очень подробно закомментирован (на данный момент только на русском языке).

В отличие от предыдущей версии расширения (`pg_self_query`), текущее переписано с нуля и не зависит от модуля `pg_query_state` и патчей ядра PostgreSQL. Таким образом может быть установлено полностью изолированно как на ванильную версию Postgres,
так и скорей всего на любой форк.

## Архитектура

Накладные расходы на горячем пути: **5 combined Start+End branches** на aarch64 (~5-7 ns/query, byte-identical disassembly на PG 16/17/18):

- **ExecutorStart**: push `queryDesc` в 100-слотовый BSS ring (одна 8-байтовая запись) — без копирования текста, без parent-указателя. 2 branches (active flag + overflow check).
- **ExecutorEnd**: декремент ring head (или `overflow_depth` на overflow-пути). 3 branches.
- **SubXactCallback**: per-subxid LIFO snapshot/restore ring head — cleanup "проглоченных" исключений в PL/pgSQL EXCEPTION-блоках, когда `standard_ExecutorEnd` не вызывается.
- **XactCallback**: сброс 3-х int-переменных BSS при COMMIT/ABORT.

Функция `pg_query_stack()` читает ring в момент вызова: snapshot-once на FIRSTCALL, lazy `pstrdup` для каждого слота, три UAF-guard'а (null queryDesc / null estate / null sourceText) — на guard выдаётся placeholder-строка вместо SIGSEGV.

**Footprint памяти:** `pgs_ring[100]` (800 B) + `pgs_ring_subxact_snap[256]` (2048 B) + 3 int = 2860 B BSS на backend, zero-initialized.

**Overflow-трекинг:** при превышении глубины 100 инкрементируется `pgs_ring_overflow_depth` вместо push в ring. ExecutorEnd зеркально декрементирует `overflow_depth`, предотвращая underrun ring head.

**Parallel workers:** пропускаются через `IsParallelWorker()` — повторно выполняемые фрагменты иначе дублировались бы в ring.

## Возможности

- Получение полного стека вложенных SQL-запросов текущего сеанса.
- Независимость от других модулей (не зависит от `pg_query_state` как прошлая версия).
- Совместимость с PostgreSQL 16 / 17 / 18 (проверено на release и cassert сборках).
- Накладные расходы ~10x ниже чем в предыдущей версии (~5-7 ns/query).
- Подробно закомментированный исходный код (пока только на русском языке).

## Совместимость

**Проверено на PostgreSQL 16.13 / 17.9 / 18.3** (release + cassert сборки, Linux aarch64 и macOS arm64). Публичный API, используемый walker'ом — `ActivePortal`, `QueryDesc`, `EState->estate` — ABI-стабилен на этих версиях. Совместимость с более ранними версиями может потребовать незначительных изменений в исходном коде.

## Варианты использования

Использование этого модуля может быть полезно в следующих ситуациях:
 - Логирование (через триггеры) запросов, которые произвели изменение данных в таблице.
 - Получить фактический результат DSQL запроса который выполнился и записать куда-либо.
 - Отладка очень сложных функций или триггеров со сложными цепочками вызовов.

## Установка

1. После клонирования данного репозитория и перехода в папку с расширением, выполните:
    ```bash
    make install USE_PGXS=1
    ```
   Убедитесь, что `pg_config` указывает на нужную версию Postgres если у вас их установлено несколько!

2. Затем измените значение параметра `session_preload_libraries` в `postgresql.conf`:
    ```
    session_preload_libraries = 'pg_query_stack'
    ```
   Расширение должно загружаться именно в сессию! Не прописывайте его в `shared_preload_libraries`.

3. Перезапустите PostgreSQL:
    ```bash
    sudo systemctl restart postgresql  # или другой способ перезапуска
    ```

4. Запустите в целевой базе, чтобы создать расширение:
    ```sql
    CREATE EXTENSION pg_query_stack;
    ```

5. Готово!

## Описание функции `pg_query_stack`

```sql
pg_query_stack(_skip_count int DEFAULT 1)
    RETURNS TABLE (
        frame_number integer,
        query_text text
    )
```

В результате выполнения функции будет выдан табличный результат стека запросов начиная от запроса верхнего уровня (0-й фрейм) и до самого нижнего уровня (N-й фрейм) минус пропущенные фреймы (см. `_skip_count`).

Что значит параметр `_skip_count`:

- `0` — возвращает весь стек запросов "как есть", включая запрос где собственно происходит вызов `pg_query_stack`.
- `1` — (умолчание) возвращает стек без запроса, где происходит собственно вызов `pg_query_stack`.
- `N` — будет пропущено указанное количество запросов в стеке начиная с нижнего уровня.

## Пример работы расширения

Создадим две функции в базе:

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

Теперь вызовем функцию `test_up`:

```sql
SELECT test_up();
```

Теперь посмотрим что у нас запишется в таблицу `test1`:

```sql
SELECT * FROM test1;
```

Результат:

| frame_number | query_text       |
|--------------|------------------|
| 1            | SELECT test()    |
| 0            | SELECT test_up() |

Таблица `test1` содержит стек запросов без текущего вызова `pg_query_stack`, так как параметр `_skip_count` имеет значение по умолчанию `1`.

Теперь посмотрим что у нас запишется в таблицу `test2`:

```sql
SELECT * FROM test2;
```

Результат:

| frame_number | query_text                                                 |
|--------------|------------------------------------------------------------|
| 2            | CREATE TEMP TABLE test2 AS SELECT * FROM pg_query_stack(0) |
| 1            | SELECT test()                                              |
| 0            | SELECT test_up()                                           |

Таблица `test2` содержит полный стек запросов, включая текущий вызов `pg_query_stack`, поскольку мы передали параметр `_skip_count = 0`.

**Пояснение:**

В первом случае мы получаем стек без текущего запроса `pg_query_stack()`, что может быть полезно для получения информации о внешних запросах.
Во втором случае мы видим полный стек, включая самый внутренний вызов, что позволяет полностью проследить цепочку вызовов.

## Обновление версии расширения

После компиляции из исходных файлов выполните:

```sql
DROP EXTENSION pg_query_stack;
CREATE EXTENSION pg_query_stack;
```

Затем перезагрузите PostgreSQL.

## Переход с расширения `pg_self_query`

Если у вас уже применялось расширение `pg_self_query` с одноименной функцией, то для обеспечения обратной совместимости и избежания необходимости менять код, можно сделать "обёртку":

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

На данный момент она автоматически создаётся при установке расширения.

## Лицензия

Этот проект лицензирован под лицензией MIT License.
Это означает, что вы можете свободно использовать, изменять и распространять этот код в коммерческих и некоммерческих целях,
при условии сохранения уведомления об авторских правах.

Полный текст лицензии доступен в файле [LICENSE](LICENSE.md).
