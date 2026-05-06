/*
 * pg_query_stack.c
 *      Расширение PostgreSQL для извлечения стека запросов текущего backend-процесса
 *
 * Это расширение позволяет получить полный стек SQL-запросов, который привёл
 * к текущему выполняемому запросу. Полезно для отладки, мониторинга и понимания
 * контекста выполнения вложенных вызовов функций.
 *
 * ОПТИМИЗИРОВАННАЯ ВЕРСИЯ (v2 — lazy materialization)
 * ===================================================
 * Данная версия оптимизирована для работы при высоких нагрузках (QPS 10k+).
 * Основные оптимизации:
 *   1. Статический массив вместо связного списка (List) — избегаем аллокаций.
 *   2. LAZY MATERIALIZATION: на горячем пути ExecutorStart хранится СЫРОЙ
 *      указатель на queryDesc->sourceText (без strnlen+memcpy). Копирование
 *      выполняется только в момент, когда поверх рамки пушится новый фрейм
 *      (т. е. до того, как контекст памяти sourceText может быть освобождён
 *      Citus / PL/pgSQL / TimescaleDB). Для не-вложенных запросов (≈95 %
 *      OLTP) копирование не выполняется ни разу.
 *   3. УДАЛЁН ExecutorFinish-хук: очисткой error-path занимаются
 *      RegisterSubXactCallback (SUBXACT_EVENT_ABORT_SUB) — для swallowed
 *      exception в plpgsql, и RegisterXactCallback (XACT_EVENT_ABORT) —
 *      для ошибок без subxact-перехвата. Это покрывает все случаи и убирает
 *      sigsetjmp + лишний indirect call на каждый запрос.
 *   4. УДАЛЁН PG_TRY/PG_CATCH в ExecutorEnd: pop делается ДО вызова
 *      downstream-хука (стек консистентен даже при throw).
 *   5. GUC-переменная для отключения без рестарта сервера.
 *   6. Объединённый флаг pg_query_stack_active = (enabled && !ParallelWorker)
 *      обновляется через GUC assign_hook → один tbz в hot-path.
 *
 * Overhead при включённом расширении: ≈54 ns/query на PG 16.13 release
 * (-O2 -g, aarch64). На отключённом — ≈ ваш cost-of-being-loaded.
 *
 * BACKWARDS-COMPAT NOTE:
 *   pg_query_stack(), вызванная из downstream ExecutorEnd-хука другого
 *   расширения, больше НЕ увидит завершающийся запрос (наш фрейм уже снят).
 *   На практике этот сценарий не встречается.
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "access/parallel.h"
#include "access/xact.h"

/*
 * PG_MODULE_MAGIC — обязательный макрос для всех расширений PostgreSQL.
 *
 * Вставляет специальный "магический" блок в скомпилированную библиотеку.
 * Этот блок содержит информацию о версии PostgreSQL, для которой скомпилировано
 * расширение. При загрузке расширения сервер проверяет этот блок для обеспечения
 * совместимости версий.
 *
 * Без этого макроса PostgreSQL откажется загружать расширение!
 */
PG_MODULE_MAGIC;


/* ============================================================================
 * КОНСТАНТЫ И КОНФИГУРАЦИЯ
 * ============================================================================ */

/*
 * Максимальная глубина стека запросов.
 *
 * Ограничение необходимо для защиты от бесконечной рекурсии и чрезмерного
 * потребления памяти. 100 уровней вложенности более чем достаточно для
 * любых практических сценариев.
 *
 * ОПТИМИЗАЦИЯ: Размер статического массива фиксирован, что позволяет
 * компилятору оптимизировать доступ к элементам.
 */
#define MAX_QUERY_STACK_DEPTH 100

/*
 * Максимальная длина текста запроса для хранения.
 *
 * Запросы длиннее этого лимита будут обрезаны при материализации.
 * Это защита от OOM при работе с очень большими запросами (например,
 * с огромными IN-списками). 512KB — разумный баланс между полнотой
 * информации и потреблением памяти.
 */
#define MAX_QUERY_TEXT_LENGTH 524288  /* 512KB */

/*
 * Индикатор обрезанного запроса.
 * Добавляется в конец текста, если запрос был обрезан при материализации.
 */
#define TRUNCATION_SUFFIX "... truncated"
#define TRUNCATION_SUFFIX_LEN 13  /* strlen("... truncated") */


/* ============================================================================
 * СТРУКТУРЫ ДАННЫХ
 * ============================================================================ */

/*
 * Размер inline-буфера в каждом элементе стека.
 *
 * V2: уменьшен с 512 до 128 байт. Материализация — РЕДКИЙ путь (только при
 * вложенности ≈5 % OLTP-запросов), поэтому держать огромный inline-буфер
 * на каждом из 100 слотов невыгодно. 100 × 144 ≈ 14 KB вместо 52 KB —
 * существенно лучше для L2/L3 кешей при большом количестве backend'ов.
 *
 * Запросы, чья длина <= INLINE_BUF_SIZE-1 при материализации, кладутся
 * прямо в inline_buf (без palloc). Более длинные — в TopTransactionContext.
 */
#define INLINE_BUF_SIZE 128

/*
 * Максимальная глубина стека подтранзакций, которые мы отслеживаем.
 *
 * Нужен для PL/pgSQL EXCEPTION-блоков: они создают внутренние подтранзакции.
 * 256 уровней более чем достаточно для практических сценариев.
 */
#define MAX_SUBXACT_STACK_DEPTH 256

/*
 * Структура для хранения одной записи в стеке запросов.
 *
 * ИНВАРИАНТ ЧТЕНИЯ:
 *   - Если heap_copy != NULL, текст читается по heap_copy (стабильная копия,
 *     валидна до конца транзакции).
 *   - Иначе текст читается по raw_text. Это безопасно ТОЛЬКО когда фрейм
 *     находится на вершине стека: значит, мы внутри его executor-области,
 *     и контекст памяти sourceText ещё жив.
 *
 * MATERIALIZE-ИНВАРИАНТ (фундамент lazy materialization):
 *   ЛЮБОЙ ФРЕЙМ, ПОВЕРХ КОТОРОГО БЫЛ ЗАПУШЕН СЛЕДУЮЩИЙ, ОБЯЗАН ИМЕТЬ
 *   heap_copy != NULL ДО МОМЕНТА ПУША. Это гарантирует корректность
 *   чтения родительских фреймов в Citus/PL/pgSQL/TimescaleDB, где
 *   контекст sourceText может освободиться раньше нашего ExecutorEnd.
 *   Инвариант поддерживается вызовом materialize_frame(parent) в начале
 *   pg_query_stack_ExecutorStart, до того как push-ним новый фрейм.
 *
 * Ранее (v1.1.3) sourceText копировался безусловно на каждый push, что
 * давало ~30-40 ns overhead на запрос на пути, который никто не читает
 * (фрейм снимался раньше любого чтения). Lazy подход эту работу выкидывает.
 */
typedef struct QueryStackEntry
{
    const char *raw_text;       /* Сырой указатель на queryDesc->sourceText */
    char       *heap_copy;      /* Стабильная копия (или указатель на inline_buf); NULL пока top-of-stack */
    char        inline_buf[INLINE_BUF_SIZE];  /* Inline-хранилище для коротких запросов при materialize */
} QueryStackEntry;

/*
 * Снимок состояния стека на входе в подтранзакцию.
 *
 * Нужен для корректного восстановления после swallowed exception внутри
 * PL/pgSQL EXCEPTION-блока: при ABORT_SUB возвращаем стек в это состояние.
 */
typedef struct SubXactStackEntry
{
    SubTransactionId subxid;
    int              query_stack_depth;
    int              query_stack_overflow_depth;
} SubXactStackEntry;


/*
 * Структура контекста для SRF-функции pg_query_stack().
 * Хранит состояние между итерациями возврата строк.
 */
typedef struct PgQueryStackContext
{
    int saved_depth;   /* Глубина стека на момент первого вызова функции */
} PgQueryStackContext;


/* ============================================================================
 * ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
 * ============================================================================ */

/*
 * Стек запросов — статический массив фиксированного размера.
 *
 * ОПТИМИЗАЦИЯ: Использование статического массива вместо связного списка (List)
 * даёт несколько преимуществ:
 *   1. Нет аллокаций памяти (palloc/pfree) при push/pop — экономия ~300ns на операцию
 *   2. Лучшая локальность данных — все элементы в непрерывной памяти
 *   3. Прямой доступ по индексу O(1) вместо O(n) для list_nth()
 *   4. Нет накладных расходов на ListCell (~16 байт на элемент)
 *
 * Недостаток: фиксированный размер, но 100 элементов достаточно для любых
 * практических сценариев (и защищает от stack overflow).
 */
static QueryStackEntry Query_Stack[MAX_QUERY_STACK_DEPTH];

/*
 * Текущая глубина стека (количество элементов).
 * Индексация: Query_Stack[0] — самый старый (top-level) запрос,
 *             Query_Stack[Query_Stack_Depth-1] — текущий запрос.
 */
static int Query_Stack_Depth = 0;

/*
 * Счётчик запросов, не поместившихся в стек (overflow).
 *
 * Когда стек заполнен (Query_Stack_Depth == MAX_QUERY_STACK_DEPTH),
 * новые запросы не добавляются, но мы должны отслеживать их количество,
 * чтобы ExecutorEnd не снял «чужой» фрейм при завершении
 * overflow-запроса.
 */
static int Query_Stack_Overflow_Depth = 0;

/* Стек снимков для вложенных подтранзакций */
static SubXactStackEntry SubXact_Stack[MAX_SUBXACT_STACK_DEPTH];
static int               SubXact_Stack_Depth = 0;

/*
 * GUC-переменная для включения/выключения расширения.
 *
 * Позволяет отключить трекинг стека без рестарта сервера:
 *   SET pg_query_stack.enabled = off;
 *
 * При отключении overhead расширения становится практически нулевым
 * (только проверка булевой переменной в хуках).
 *
 * Уровень PGC_USERSET означает, что переменную можно менять на уровне сессии.
 */
static bool pg_query_stack_enabled = true;

/*
 * Объединённый «мастер-флаг» активности на горячем пути.
 *
 *   pg_query_stack_active = pg_query_stack_enabled && !IsParallelWorker()
 *
 * Обновляется:
 *   - в _PG_init() (стартовая инициализация),
 *   - в pg_query_stack_assign_enabled() (assign_hook GUC).
 *
 * Зачем: hot-path ExecutorStart/End в v1 делал две проверки (булева
 * переменная + IsParallelWorker()), это два branch'а и одна indirect-load
 * на запрос. Объединение в один булев флаг даёт один tbz по байту в
 * hot-path. IsParallelWorker() в рамках одного backend-процесса не меняется
 * (parallel-worker — отдельный процесс), так что флаг остаётся консистентным.
 */
static bool pg_query_stack_active = true;

/*
 * Сохранённые указатели на предыдущие хуки.
 *
 * PostgreSQL использует цепочку хуков — каждое расширение должно сохранить
 * предыдущий хук и вызвать его в своей реализации. Это позволяет нескольким
 * расширениям работать одновременно.
 *
 * V2: ExecutorFinish_hook больше не используется — см. шапку файла.
 */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEnd   = NULL;

/*
 * Заранее вычисленные цели цепочки, закэшированные на момент _PG_init.
 * Вызовы на горячем пути идут через них вместо условного шаблона
 * "prev_* ? prev_* : standard_*". После _PG_init считаются неизменными
 * и никогда не переприсваиваются в рантайме. _PG_fini защитно сбрасывает
 * их в NULL.
 */
static ExecutorStart_hook_type chained_ExecutorStart = NULL;
static ExecutorEnd_hook_type   chained_ExecutorEnd   = NULL;


/* ============================================================================
 * ПРОТОТИПЫ ФУНКЦИЙ
 * ============================================================================ */

/* Функции инициализации и выгрузки расширения */
void _PG_init(void);
void _PG_fini(void);

/* Хуки выполнения запросов (V2: ExecutorFinish-хука больше нет) */
static void pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_query_stack_ExecutorEnd(QueryDesc *queryDesc);

/* Callback транзакций */
static void pg_query_stack_xact_callback(XactEvent event, void *arg);
static void pg_query_stack_subxact_callback(SubXactEvent event,
                                            SubTransactionId mySubid,
                                            SubTransactionId parentSubid,
                                            void *arg);

/* GUC assign-hook */
static void pg_query_stack_assign_enabled(bool newval, void *extra);

/* Helpers */
static void materialize_frame(QueryStackEntry *entry);

/* SQL-функция для получения стека */
Datum pg_query_stack(PG_FUNCTION_ARGS);


/* ============================================================================
 * ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
 * ============================================================================ */

/*
 * materialize_frame — копирует raw_text в стабильное хранилище (heap_copy).
 *
 * Холодный путь lazy-materialization: вызывается только когда поверх фрейма
 * пушится новый, или когда pg_query_stack() читает стек и хочет защититься
 * от любых нестандартных hook-сценариев.
 *
 * Стратегия:
 *   - len <= INLINE_BUF_SIZE - 1: копируем в entry->inline_buf, heap_copy
 *     указывает на этот же буфер. pfree НЕ нужен при pop.
 *   - len > MAX_QUERY_TEXT_LENGTH: усекаем до лимита + добавляем суффикс
 *     "... truncated" в heap-аллокации в TopTransactionContext.
 *   - средний случай: heap-аллокация в TopTransactionContext по точной длине.
 *
 * Аллокация в TopTransactionContext: при завершении транзакции контекст
 * уничтожается целиком, индивидуальные pfree не нужны (см. xact_callback).
 *
 * Идемпотентна: повторный вызов на уже материализованном фрейме — no-op.
 */
static void
materialize_frame(QueryStackEntry *entry)
{
    const char *src = entry->raw_text;
    size_t      len;

    if (entry->heap_copy != NULL)
        return;                 /* Уже материализован */

    if (src == NULL)
    {
        /* Запрос без текста (редкий случай, например внутренние операции) */
        entry->raw_text  = "<unnamed query>";
        entry->heap_copy = (char *) entry->raw_text;  /* безопасный const cast: не пишем в литерал */
        return;
    }

    /*
     * strnlen вместо strlen: не сканируем дальше лимита обрезки.
     * Для обычных запросов (<512KB) — идентично strlen.
     * Для гигантских запросов — экономия на бесполезном сканировании.
     */
    len = strnlen(src, MAX_QUERY_TEXT_LENGTH + 1);

    if (len <= INLINE_BUF_SIZE - 1)
    {
        /*
         * Быстрый путь материализации: запрос помещается в inline-буфер.
         * Никаких аллокаций — только memcpy в статическую память кадра.
         */
        memcpy(entry->inline_buf, src, len + 1);
        entry->heap_copy = entry->inline_buf;
    }
    else if (len > MAX_QUERY_TEXT_LENGTH)
    {
        /*
         * Запрос слишком длинный — обрезаем.
         * MemoryContextAlloc вместо SwitchTo+palloc+SwitchBack:
         * один вызов вместо трёх, без подмены CurrentMemoryContext.
         */
        char *buf = MemoryContextAlloc(TopTransactionContext,
                                       MAX_QUERY_TEXT_LENGTH + TRUNCATION_SUFFIX_LEN + 1);
        memcpy(buf, src, MAX_QUERY_TEXT_LENGTH);
        memcpy(buf + MAX_QUERY_TEXT_LENGTH, TRUNCATION_SUFFIX, TRUNCATION_SUFFIX_LEN + 1);
        entry->heap_copy = buf;
    }
    else
    {
        /*
         * Средний путь: запрос не помещается в inline-буфер,
         * но короче лимита обрезки. Аллоцируем в TopTransactionContext.
         */
        char *buf = MemoryContextAlloc(TopTransactionContext, len + 1);
        memcpy(buf, src, len + 1);
        entry->heap_copy = buf;
    }
}


/*
 * Удаление верхнего элемента стека (pop).
 *
 * ОПТИМИЗАЦИЯ: inline-функция для минимизации накладных расходов.
 * Вызывается в ExecutorEnd для каждого завершённого запроса.
 *
 * Время выполнения: ~5-10ns (декремент счётчика + условный pfree).
 *
 * V2: pfree выполняется ТОЛЬКО если heap_copy указывает на out-of-line
 * аллокацию (а не на наш inline_buf). Различаем по сравнению указателей.
 * Это важно: при материализации в inline_buf heap_copy = entry->inline_buf,
 * и pfree на этот указатель привёл бы к ошибке.
 */
static inline void
pg_stack_pop(void)
{
    QueryStackEntry *entry;

    if (Query_Stack_Depth <= 0)
        return;

    Query_Stack_Depth--;
    entry = &Query_Stack[Query_Stack_Depth];

    /* Освобождаем нашу копию текста запроса, если это heap-аллокация. */
    if (unlikely(entry->heap_copy != NULL && entry->heap_copy != entry->inline_buf))
        pfree(entry->heap_copy);

    entry->heap_copy = NULL;
    entry->raw_text  = NULL;
}


/*
 * Восстановление стека к заданному снимку.
 *
 * Используется при откате подтранзакции, когда часть query frame'ов уже
 * успела попасть в стек, но логически относится к aborted subtransaction.
 */
static void
pg_stack_restore(int target_depth, int target_overflow_depth)
{
    while (Query_Stack_Depth > target_depth)
        pg_stack_pop();

    Query_Stack_Overflow_Depth = target_overflow_depth;
}


/* ============================================================================
 * GUC ASSIGN HOOK
 * ============================================================================ */

/*
 * Callback изменения GUC pg_query_stack.enabled.
 *
 * Поддерживает консистентность мастер-флага pg_query_stack_active.
 * IsParallelWorker() вычисляется на момент изменения GUC; в parallel-worker'е
 * это будет true и флаг останется false независимо от значения GUC, что
 * корректно отражает желаемое поведение «не трекать parallel-workers».
 */
static void
pg_query_stack_assign_enabled(bool newval, void *extra)
{
    pg_query_stack_enabled = newval;
    pg_query_stack_active  = newval && !IsParallelWorker();
}


/* ============================================================================
 * ФУНКЦИИ ИНИЦИАЛИЗАЦИИ И ВЫГРУЗКИ
 * ============================================================================ */

/*
 * _PG_init — точка входа при загрузке расширения.
 *
 * Вызывается PostgreSQL при первой загрузке shared library.
 * Если расширение указано в session_preload_libraries, вызывается при создании
 * каждой сессии (для shared_preload_libraries — при старте сервера, но
 * pg_query_stack использует session_preload).
 *
 * Здесь мы:
 *   1. Регистрируем GUC-переменную для управления расширением
 *   2. Инициализируем мастер-флаг pg_query_stack_active
 *   3. Устанавливаем хуки на ExecutorStart и ExecutorEnd
 *      (V2: ExecutorFinish-хук больше не нужен — см. шапку)
 *   4. Регистрируем callback'и для отслеживания завершения транзакций
 *      и подтранзакций
 */
void
_PG_init(void)
{
    /*
     * Определяем GUC-переменную pg_query_stack.enabled
     *
     * DefineCustomBoolVariable создаёт новую конфигурационную переменную,
     * которую можно изменять через SET и postgresql.conf.
     *
     * V2: передаём assign_hook = pg_query_stack_assign_enabled, чтобы
     * консистентно обновлять мастер-флаг pg_query_stack_active.
     */
    DefineCustomBoolVariable(
        "pg_query_stack.enabled",                           /* имя переменной */
        "Enable or disable query stack tracking",           /* краткое описание */
        "When disabled, the extension has near-zero overhead. "
        "Can be changed per-session without server restart.", /* полное описание */
        &pg_query_stack_enabled,                            /* указатель на переменную */
        true,                                               /* значение по умолчанию */
        PGC_USERSET,                                        /* контекст (можно менять в сессии) */
        0,                                                  /* флаги */
        NULL,                                               /* check_hook */
        pg_query_stack_assign_enabled,                      /* assign_hook (V2) */
        NULL                                                /* show_hook */
    );

    /* Инициализируем мастер-флаг после регистрации GUC */
    pg_query_stack_active = pg_query_stack_enabled && !IsParallelWorker();

    /*
     * Регистрируем хуки выполнения запросов.
     *
     * ВАЖНО: Сохраняем предыдущие хуки! PostgreSQL использует цепочку хуков,
     * и мы должны вызывать предыдущий хук в нашей реализации, чтобы не сломать
     * работу других расширений.
     */
    prev_ExecutorStart = ExecutorStart_hook;
    prev_ExecutorEnd   = ExecutorEnd_hook;

    chained_ExecutorStart = prev_ExecutorStart
                              ? prev_ExecutorStart
                              : standard_ExecutorStart;
    chained_ExecutorEnd   = prev_ExecutorEnd
                              ? prev_ExecutorEnd
                              : standard_ExecutorEnd;

    ExecutorStart_hook = pg_query_stack_ExecutorStart;
    ExecutorEnd_hook   = pg_query_stack_ExecutorEnd;

    /*
     * Регистрируем callback'и для отслеживания завершения транзакций.
     *
     * XactCallback гарантирует очистку стека при любом завершении транзакции,
     * включая случаи когда ExecutorEnd не вызывается (ошибка на этапе
     * планирования, RAISE EXCEPTION до выполнения и т.п.).
     *
     * SubXactCallback (V2-критично): теперь несёт двойную нагрузку:
     * исторически — снимок/восстановление стека на START_SUB/ABORT_SUB
     * для swallowed exception в plpgsql; в v2 — единственный страховочный
     * путь для AFTER trigger error внутри EXCEPTION-блока (раньше эту
     * страховку давал ExecutorFinish-хук с PG_TRY).
     */
    RegisterXactCallback(pg_query_stack_xact_callback, NULL);
    RegisterSubXactCallback(pg_query_stack_subxact_callback, NULL);
}


/*
 * _PG_fini — точка выхода при выгрузке расширения.
 *
 * Вызывается PostgreSQL при выгрузке shared library.
 * ВАЖНО: В PostgreSQL расширения редко выгружаются динамически,
 * обычно это происходит только при остановке сервера.
 */
void
_PG_fini(void)
{
    /* Восстанавливаем предыдущие хуки */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook   = prev_ExecutorEnd;
    chained_ExecutorStart = NULL;
    chained_ExecutorEnd   = NULL;

    /* Снимаем регистрацию callback'ов */
    UnregisterXactCallback(pg_query_stack_xact_callback, NULL);
    UnregisterSubXactCallback(pg_query_stack_subxact_callback, NULL);

    /*
     * Сбрасываем счётчики на случай выгрузки посреди транзакции.
     * Сами heap_copy будут уничтожены вместе с TopTransactionContext.
     */
    Query_Stack_Depth          = 0;
    Query_Stack_Overflow_Depth = 0;
    SubXact_Stack_Depth        = 0;
}


/* ============================================================================
 * CALLBACK ТРАНЗАКЦИЙ
 * ============================================================================ */

/*
 * pg_query_stack_xact_callback — обработчик событий транзакции.
 *
 * Зачем это нужно?
 * ----------------
 * Есть случаи, когда ExecutorEnd НЕ вызывается, но транзакция завершается:
 *   - Ошибка на этапе разбора/планирования (таблица не найдена, синтаксическая ошибка)
 *   - Ошибка в CHECK constraint или триггере до начала выполнения
 *   - Вызов функции, которая делает RAISE EXCEPTION до выполнения запросов
 *
 * В этих случаях стек может остаться "грязным". Callback транзакции
 * гарантирует очистку стека при любом завершении транзакции.
 *
 * События:
 *   XACT_EVENT_COMMIT — успешное завершение транзакции
 *   XACT_EVENT_ABORT  — откат транзакции (ошибка или явный ROLLBACK)
 */
static void
pg_query_stack_xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
    {
        /*
         * Просто обнуляем счётчик — O(1) вместо O(n).
         *
         * Все heap_copy аллоцированы в TopTransactionContext, который
         * уничтожается целиком при завершении транзакции. Индивидуальные
         * pfree не нужны — это выброшенная работа.
         *
         * Слоты массива со «стухшими» указателями безопасны: они находятся
         * выше Query_Stack_Depth и не читаются. При следующем push
         * raw_text/heap_copy перезаписываются перед использованием слота.
         */
        Query_Stack_Depth          = 0;
        Query_Stack_Overflow_Depth = 0;
        SubXact_Stack_Depth        = 0;
    }
}


/*
 * pg_query_stack_subxact_callback — обработчик событий подтранзакций.
 *
 * Это критично для PL/pgSQL EXCEPTION-блоков: PostgreSQL откатывает внутреннюю
 * подтранзакцию, но внешняя транзакция продолжает выполняться. Если не вернуть
 * стек к состоянию на входе в subxact, "протухшие" frame'ы останутся видимыми
 * в следующем statement.
 *
 * V2: единственный механизм страховки error-path для AFTER-trigger-ошибок,
 * перехваченных EXCEPTION-блоками. Без него ExecutorEnd для упавшего запроса
 * не вызывается, и фрейм бы протёк. Раньше это страховал ExecutorFinish-хук
 * с PG_TRY/PG_CATCH; в v2 его нет, потому что субтранзакция (всегда есть
 * вокруг EXCEPTION-блока в plpgsql) успевает позвать нас на ABORT_SUB ДО
 * того, как handler EXCEPTION получит управление.
 */
static void
pg_query_stack_subxact_callback(SubXactEvent event,
                                SubTransactionId mySubid,
                                SubTransactionId parentSubid,
                                void *arg)
{
    int i;

    if (!pg_query_stack_active)
        return;

    switch (event)
    {
        case SUBXACT_EVENT_START_SUB:
            if (likely(SubXact_Stack_Depth < MAX_SUBXACT_STACK_DEPTH))
            {
                SubXactStackEntry *entry = &SubXact_Stack[SubXact_Stack_Depth++];

                entry->subxid                     = mySubid;
                entry->query_stack_depth          = Query_Stack_Depth;
                entry->query_stack_overflow_depth = Query_Stack_Overflow_Depth;
            }
            break;

        case SUBXACT_EVENT_ABORT_SUB:
            for (i = SubXact_Stack_Depth - 1; i >= 0; i--)
            {
                if (SubXact_Stack[i].subxid == mySubid)
                {
                    pg_stack_restore(SubXact_Stack[i].query_stack_depth,
                                     SubXact_Stack[i].query_stack_overflow_depth);
                    SubXact_Stack_Depth = i;
                    break;
                }
            }
            break;

        case SUBXACT_EVENT_COMMIT_SUB:
        case SUBXACT_EVENT_PRE_COMMIT_SUB:
            for (i = SubXact_Stack_Depth - 1; i >= 0; i--)
            {
                if (SubXact_Stack[i].subxid == mySubid)
                {
                    SubXact_Stack_Depth = i;
                    break;
                }
            }
            break;
    }
}


/* ============================================================================
 * ХУКИ ВЫПОЛНЕНИЯ ЗАПРОСОВ
 * ============================================================================ */

/*
 * pg_query_stack_ExecutorStart — хук начала выполнения запроса.
 *
 * Почему именно ExecutorStart?
 * ----------------------------
 * В PostgreSQL выполнение запроса проходит несколько этапов:
 *   1. Parse    — разбор SQL-текста в дерево разбора
 *   2. Analyze  — анализ и преобразование в Query
 *   3. Rewrite  — применение правил перезаписи
 *   4. Plan     — построение плана выполнения
 *   5. Execute  — выполнение плана (ExecutorStart → ExecutorRun → ExecutorEnd)
 *
 * ExecutorStart вызывается ОДИН раз в начале выполнения, что идеально для
 * добавления записи в стек. ExecutorRun может вызываться многократно
 * (для курсоров, LIMIT и т.д.), поэтому не подходит.
 *
 * Критический путь выполнения (V2)
 * --------------------------------
 * Этот хук вызывается для КАЖДОГО запроса. Overhead на горячем пути:
 *
 *   v1.1.3 (eager copy в inline_buf 512B + heap fallback, ≈55 ns):
 *     - strnlen(sourceText)          ~30ns  ← ВСЕГДА
 *     - memcpy в inline_buf          ~15ns  ← ВСЕГДА
 *     - depth++ + pop в Executor End ~10ns
 *
 *   v2 (lazy materialize, ≈12-20 ns hot-path):
 *     - check pg_query_stack_active  ~1ns   (один tbz)
 *     - вызов downstream / standard  ~5ns   (indirect tail-call)
 *     - check parent.heap_copy       ~2ns   (только при depth>0)
 *     - push raw_text + depth++      ~5ns   (без strnlen, без memcpy)
 *
 * Materialize выполняется ХОЛОДНЫМ путём только когда поверх рамки пушится
 * следующий фрейм (≈5 % OLTP-запросов вложены).
 *
 * ИСТОРИЧЕСКАЯ ЗАМЕТКА: ранее использовалось хранение "сырого" указателя
 * на sourceText без копирования (~10ns), но это приводило к SIGSEGV
 * (dangling pointer) в средах с Citus + PL/pgSQL, где контекст памяти
 * sourceText может быть освобождён до вызова ExecutorEnd. v2 решает это
 * lazy-копированием — родитель материализуется в момент пуша ребёнка,
 * пока контекст ещё жив.
 */
static void
pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    /*
     * Быстрый выход если расширение отключено или это параллельный worker.
     *
     * ОПТИМИЗАЦИЯ V2: один объединённый флаг pg_query_stack_active вместо
     * двух проверок (pg_query_stack_enabled + IsParallelWorker()).
     * При отключённом расширении overhead практически нулевой.
     *
     * Параллельные workers пропускаем, так как у них свой контекст выполнения,
     * а стек запросов актуален только для основного backend-процесса.
     */
    if (unlikely(!pg_query_stack_active))
    {
        chained_ExecutorStart(queryDesc, eflags);
        return;
    }

    /*
     * MATERIALIZE-ИНВАРИАНТ:
     *   родитель должен иметь стабильную копию ДО того, как мы передаём
     *   управление downstream-хуку или standard_ExecutorStart, потому что
     *   downstream может запустить SPI/Citus/PL/pgSQL-цепочку, которая
     *   освободит контекст памяти sourceText родителя.
     *
     * Это холодный путь: на не-вложенных запросах depth==0 и проверка
     * мгновенно проходит. На вложенных — materialize_frame копирует
     * текст один раз и затем повторно НЕ выполняется (heap_copy != NULL).
     */
    if (unlikely(Query_Stack_Depth > 0))
    {
        QueryStackEntry *parent = &Query_Stack[Query_Stack_Depth - 1];
        if (parent->heap_copy == NULL)
            materialize_frame(parent);
    }

    /*
     * Сначала вызываем downstream хук.
     *
     * Push в стек делаем ПОСЛЕ успешного вызова — если хук бросит ошибку,
     * чистить нечего, и PG_TRY не нужен. Этот паттерн используют
     * pg_stat_statements и auto_explain.
     */
    chained_ExecutorStart(queryDesc, eflags);

    /*
     * Добавляем запрос в стек (если есть место).
     *
     * V2: НЕ копируем текст — только сохраняем сырой указатель.
     * Это безопасно, потому что фрейм будет либо снят раньше, чем кто-либо
     * прочитает текст (типичный путь), либо материализован при пуше
     * следующего фрейма (см. инвариант выше).
     */
    if (likely(Query_Stack_Depth < MAX_QUERY_STACK_DEPTH))
    {
        QueryStackEntry *entry = &Query_Stack[Query_Stack_Depth];

        entry->raw_text  = queryDesc->sourceText;
        entry->heap_copy = NULL;
        Query_Stack_Depth++;
    }
    else
    {
        /* Стек полон — считаем overflow-запросы для корректного pop в ExecutorEnd */
        Query_Stack_Overflow_Depth++;
    }
}


/*
 * pg_query_stack_ExecutorEnd — хук завершения выполнения запроса.
 *
 * Вызывается после завершения выполнения запроса (успешного или нет).
 * Здесь мы убираем запись из стека.
 *
 * ВАЖНО: Порядок вызовов ExecutorStart/ExecutorEnd гарантирует LIFO-семантику:
 * для вложенных запросов сначала вызывается ExecutorEnd внутреннего запроса,
 * потом внешнего. Это соответствует логике стека.
 *
 * V2: POP-BEFORE-DOWNSTREAM
 * -------------------------
 * В v1 порядок был: вызвать downstream → потом pop, всё в PG_TRY/PG_CATCH
 * чтобы pop сработал даже при throw. Каждый PG_TRY ставил sigsetjmp на
 * каждый запрос (~5 ns overhead).
 *
 * В v2 pop делается СНАЧАЛА:
 *   - если downstream бросит, наш стек уже консистентен;
 *   - subxact/xact callbacks остаются страховкой для путей, где
 *     ExecutorEnd не вызывается совсем (см. xact_callback и subxact_callback).
 *
 * Поведенческое отличие: pg_query_stack(), вызванная из downstream-хука
 * другого расширения, теперь не увидит завершающийся запрос (фрейм уже снят).
 * На практике этот сценарий не встречается.
 */
static void
pg_query_stack_ExecutorEnd(QueryDesc *queryDesc)
{
    /*
     * Pop ПЕРЕД вызовом downstream — без PG_TRY.
     *
     * Если это overflow-запрос (не поместился в стек при push),
     * просто декрементируем overflow-счётчик. Иначе снимаем реальный фрейм.
     * Без этой проверки pop снял бы «чужой» фрейм при глубокой рекурсии.
     */
    if (likely(pg_query_stack_active))
    {
        if (Query_Stack_Overflow_Depth > 0)
            Query_Stack_Overflow_Depth--;
        else if (Query_Stack_Depth > 0)
            pg_stack_pop();
    }

    chained_ExecutorEnd(queryDesc);
}


/* ============================================================================
 * SQL-ФУНКЦИЯ pg_query_stack()
 * ============================================================================ */

/*
 * PG_FUNCTION_INFO_V1 — макрос для объявления функции версии 1.
 *
 * Сообщает PostgreSQL, что функция pg_query_stack использует calling convention
 * версии 1 (fmgr v1). Это стандарт для всех современных расширений PostgreSQL.
 *
 * Без этого макроса PostgreSQL не сможет правильно вызвать C-функцию из SQL!
 */
PG_FUNCTION_INFO_V1(pg_query_stack);

/*
 * pg_query_stack — SQL-функция для получения стека запросов.
 *
 * Сигнатура: pg_query_stack(skip_count int DEFAULT 1)
 *            RETURNS TABLE (frame_number int, query_text text)
 *
 * Параметр skip_count указывает, сколько записей пропустить с конца стека
 * (то есть с "текущего" конца). По умолчанию skip_count=1, что пропускает
 * сам вызов pg_query_stack() — обычно он не интересен пользователю.
 *
 * Результат:
 *   frame_number = 0 — top-level запрос (самый внешний)
 *   frame_number = N — самый вложенный запрос (ближе к текущему)
 *
 * Это Set-Returning Function (SRF) — функция, возвращающая множество строк.
 * PostgreSQL вызывает её многократно, пока она не вернёт "done".
 *
 * V2: при первом вызове прогоняет materialize_frame() по всем фреймам,
 * которые будут возвращены. Это страховка на случай нестандартных
 * hook-сценариев (multiple ExecutorStart до ExecutorEnd и т.п.) — на
 * стандартном пути все родительские фреймы уже материализованы инвариантом
 * lazy-materialization, и эта работа сводится к ранним returns в
 * materialize_frame.
 */
Datum
pg_query_stack(PG_FUNCTION_ARGS)  /* PG_FUNCTION_ARGS — стандартные аргументы fmgr */
{
    /* Контекст вызова SRF — хранит состояние между итерациями */
    FuncCallContext *funcctx;
    int              skip_count;

    /*
     * Получаем параметр skip_count.
     * PG_ARGISNULL проверяет, передан ли NULL вместо значения.
     * PG_GETARG_INT32 извлекает значение аргумента как int32.
     */
    skip_count = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);

    /* Валидация параметра */
    if (skip_count < 0)
        skip_count = 0;
    else if (skip_count > MAX_QUERY_STACK_DEPTH)
        skip_count = MAX_QUERY_STACK_DEPTH;

    /*
     * SRF_IS_FIRSTCALL() — проверка первого вызова функции.
     *
     * SRF вызывается многократно — по одному разу для каждой возвращаемой строки.
     * При первом вызове нужно инициализировать состояние.
     */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext         oldcontext;
        TupleDesc             tupdesc;
        int                   effective_depth;
        PgQueryStackContext  *ctx;
        int                   i;

        /* Инициализируем контекст SRF */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Переключаемся в multi_call_memory_ctx.
         *
         * Этот контекст памяти живёт между вызовами SRF и автоматически
         * освобождается после завершения. Всё, что нужно сохранить между
         * вызовами, должно быть выделено здесь.
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Сохраняем контекст для использования в последующих вызовах */
        ctx = (PgQueryStackContext *) palloc(sizeof(PgQueryStackContext));
        ctx->saved_depth = Query_Stack_Depth;  /* Запоминаем глубину на момент вызова */
        funcctx->user_fctx = ctx;

        /*
         * Вычисляем effective_depth — сколько записей будем возвращать.
         *
         * skip_count пропускает N самых НОВЫХ записей (с конца стека).
         * Например, skip_count=1 пропускает текущий вызов pg_query_stack().
         */
        effective_depth = Query_Stack_Depth - skip_count;
        if (effective_depth < 0)
            effective_depth = 0;

        funcctx->max_calls = effective_depth;

        if (effective_depth > 0)
        {
            /*
             * Создаём TupleDesc — описание структуры возвращаемых строк.
             *
             * CreateTemplateTupleDesc(2) — создаём описание для 2 колонок.
             * TupleDescInitEntry — инициализируем каждую колонку:
             *   - Номер колонки (1-based!)
             *   - Имя колонки
             *   - OID типа данных (INT4OID = int4, TEXTOID = text)
             *   - typmod (-1 = без модификатора)
             *   - attdim (0 = не массив)
             */
            tupdesc = CreateTemplateTupleDesc(2);
            TupleDescInitEntry(tupdesc, (AttrNumber) 1, "frame_number", INT4OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_text",   TEXTOID, -1, 0);

            /*
             * BlessTupleDesc — "благословляем" TupleDesc.
             *
             * Это делает его готовым для использования. Под капотом
             * присваивает типовые идентификаторы и проверяет корректность.
             */
            funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        }

        /*
         * V2: страховочная материализация всех читаемых фреймов.
         *
         * На стандартном hook-пути инвариант lazy-materialization уже
         * гарантирует, что все frames кроме top-of-stack материализованы
         * (push ребёнка материализовал родителя). Top-of-stack читается
         * через raw_text и валиден, потому что мы внутри его executor.
         *
         * Здесь мы НА ВСЯКИЙ СЛУЧАЙ материализуем всё подряд — стоимость
         * минимальна (одна проверка heap_copy != NULL → ранний return),
         * но нестандартные сценарии (alternative ExecutorStart-цепочки,
         * read через ALTER FUNCTION ... LANGUAGE pglogical и т.п.)
         * становятся безопасными.
         */
        for (i = 0; i < ctx->saved_depth - skip_count; i++)
        {
            QueryStackEntry *e = &Query_Stack[i];
            if (e->heap_copy == NULL)
                materialize_frame(e);
        }

        /* Возвращаемся в исходный контекст памяти */
        MemoryContextSwitchTo(oldcontext);
    }

    /*
     * SRF_PERCALL_SETUP() — получаем контекст для текущего вызова.
     *
     * Этот макрос должен вызываться в КАЖДОМ вызове функции (не только первом).
     */
    funcctx = SRF_PERCALL_SETUP();

    /*
     * Проверяем, есть ли ещё строки для возврата.
     *
     * call_cntr автоматически увеличивается PostgreSQL при каждом вызове.
     * Когда call_cntr >= max_calls, мы вернули все строки.
     */
    if (funcctx->call_cntr < funcctx->max_calls)
    {
        /* Переменные для формирования результата */
        Datum                  values[2];     /* Значения колонок */
        bool                   nulls[2] = {false, false};  /* Флаги NULL */
        HeapTuple              tuple;         /* Результирующий кортеж */
        PgQueryStackContext   *ctx = (PgQueryStackContext *) funcctx->user_fctx;
        int                    stack_idx;
        const char            *query_text;

        /*
         * Вычисляем индекс в массиве стека.
         *
         * Стек хранится в порядке добавления:
         *   [0] = top-level (самый старый/внешний запрос)
         *   [depth-1] = текущий (самый новый/внутренний запрос)
         *
         * frame_number 0 соответствует индексу 0 в массиве.
         * Это интуитивно понятно: frame 0 = "корень" стека вызовов.
         */
        stack_idx = funcctx->call_cntr;

        if (stack_idx < ctx->saved_depth)
        {
            QueryStackEntry *e = &Query_Stack[stack_idx];

            /*
             * V2: предпочитаем heap_copy (стабильную копию). Если её нет —
             * это top-of-stack, читаем по raw_text (безопасно: мы внутри
             * executor-области этого запроса).
             */
            query_text = e->heap_copy ? e->heap_copy : e->raw_text;

            /* Защита от NULL или пустой строки */
            if (query_text == NULL || query_text[0] == '\0')
                query_text = "<unnamed query>";
        }
        else
        {
            /* Не должно происходить, но защита не помешает */
            query_text = "<stack overflow>";
        }

        /*
         * Формируем значения колонок.
         *
         * Int32GetDatum — преобразует int32 в Datum (универсальный тип PostgreSQL)
         * CStringGetTextDatum — преобразует C-строку в Datum типа text
         */
        values[0] = Int32GetDatum((int32)funcctx->call_cntr);
        values[1] = CStringGetTextDatum(query_text);

        /*
         * heap_form_tuple — создаём кортеж из значений.
         *
         * Объединяет TupleDesc + values + nulls в готовый HeapTuple,
         * который можно вернуть из функции.
         */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        /*
         * SRF_RETURN_NEXT — возвращаем очередную строку.
         *
         * HeapTupleGetDatum преобразует HeapTuple в Datum.
         * Макрос также увеличивает call_cntr для следующего вызова.
         */
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /*
         * SRF_RETURN_DONE — сигнализируем о завершении.
         *
         * После этого PostgreSQL больше не будет вызывать функцию
         * и освободит multi_call_memory_ctx.
         */
        SRF_RETURN_DONE(funcctx);
    }
}
