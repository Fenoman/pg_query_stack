/*
 * pg_query_stack.c — расширение PostgreSQL для получения стека SQL-запросов
 * текущего backend-процесса.
 *
 * Расширение позволяет в любой момент — из триггера, PL/pgSQL-функции,
 * SPI-вызова — узнать полный список SQL-запросов, выполняющихся в данный
 * момент в этом же сеансе. Это удобно для отладки, аудита и мониторинга.
 *
 * Горячий путь (выполняется при каждом запросе):
 *   - ExecutorStart:    сохраняет указатель на QueryDesc в кольцевой буфер
 *   - ExecutorEnd:      удаляет запись из буфера
 *   - SubXactCallback:  сохраняет/восстанавливает позицию буфера при
 *                       открытии/откате вложенных транзакций (SAVEPOINT)
 *   - XactCallback:     сбрасывает буфер в ноль при завершении транзакции
 *
 * Путь чтения (вызывается пользователем через SQL):
 *   - Снимок `ring_head` фиксируется один раз в начале вызова функции
 *   - Перебираются слоты [0, max_emit) в контексте multi_call_memory_ctx
 *   - Три UAF-защиты на слот (qd / qd->estate / qd->sourceText) —
 *     при срабатывании строка-заполнитель (не NULL, чтобы не ломать счётчик)
 *   - Текст запроса копируется через pstrdup только при чтении, не при записи
 *
 * Объём памяти в BSS: 100×8 байт кольцо + 256×8 байт LIFO субтранзакций
 *                     + несколько int-переменных ≈ 2860 байт.
 *
 * Важное поведение: при вызове pg_query_stack() из ExecutorEnd-хука другого
 * расширения завершающийся запрос уже не виден — наш фрейм снимается до
 * передачи управления downstream-хуку.
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

PG_MODULE_MAGIC;


/* ============================================================================
 * Константы
 * ============================================================================ */

/*
 * Максимальная глубина кольцевого буфера запросов.
 * 100 уровней хватает для любого практического сценария вложенности.
 * Если реальная глубина превысит это значение, лишние запросы не теряются —
 * они учитываются счётчиком pgs_ring_overflow_depth, чтобы ExecutorEnd
 * мог корректно синхронизировать pop без подрыва ring_head.
 */
#define MAX_QUERY_STACK_DEPTH 100

/*
 * Максимальная глубина LIFO-стека снимков субтранзакций.
 * Каждый SAVEPOINT (или PL/pgSQL EXCEPTION-блок) добавляет одну запись.
 * 256 уровней вложенности — практически недостижимый предел для реального кода.
 */
#define MAX_SUBXACT_STACK_DEPTH 256


/* ============================================================================
 * Глобальные переменные
 * ============================================================================ */

/*
 * GUC-параметр pg_query_stack.enabled.
 * Позволяет отключить трекинг на уровне отдельного сеанса без перезапуска
 * сервера: SET pg_query_stack.enabled = off;
 * Когда расширение отключено, накладные расходы на горячем пути близки к нулю.
 */
static bool pg_query_stack_enabled = true;

/*
 * Объединённый флаг горячего пути: enabled && !IsParallelWorker().
 *
 * Почему не проверять IsParallelWorker() прямо в хуках?
 * IsParallelWorker() — константа на всё время жизни процесса, поэтому
 * достаточно вычислить её один раз при инициализации (и при смене GUC).
 * Один флаг pg_query_stack_active — это одна ветка вместо двух на горячем
 * пути, что ощутимо при тысячах коротких запросов в секунду.
 *
 * Почему параллельные воркеры исключаются?
 * Параллельные воркеры повторно выполняют фрагменты того же плана, что и
 * лидер. Если бы они писали в кольцевой буфер, один и тот же запрос
 * отображался бы несколько раз, а счётчик ring_head рассинхронизировался бы
 * между лидером и воркерами (у каждого процесса своя BSS-область).
 */
static bool pg_query_stack_active = true;

/* ============================================================================
 * Основные структуры данных: кольцевой буфер + LIFO-стек субтранзакций
 * ============================================================================ */

/*
 * PgsRingSlot — одна ячейка кольцевого буфера.
 *
 * Хранит только указатель на QueryDesc текущего запроса.
 * Текст запроса (qd->sourceText) намеренно НЕ копируется здесь:
 * копирование — дорогая операция, а pg_query_stack() вызывается редко.
 * Поэтому текст копируется через pstrdup только в момент чтения (SRF).
 */
typedef struct PgsRingSlot
{
    QueryDesc *qd;
} PgsRingSlot;

/*
 * Кольцевой буфер активных запросов бэкенда.
 * Размещён в BSS (статика, инициализируется нулями — heap-аллокация не нужна).
 * Объём: 100 × 8 байт = 800 байт.
 *
 * pgs_ring_head — текущая глубина заполнения (= глубина стека запросов).
 * ExecutorStart делает push (ring[head].qd = qd; head++).
 * ExecutorEnd делает pop (head--).
 */
static PgsRingSlot pgs_ring[MAX_QUERY_STACK_DEPTH];
static int         pgs_ring_head = 0;

/*
 * Счётчик «лишних» вызовов ExecutorStart, не поместившихся в кольцо.
 *
 * Почему отдельный счётчик, а не просто игнорировать overflow?
 * Если ExecutorStart пропустил push (depth >= MAX), то ExecutorEnd для этого
 * же запроса должен тоже пропустить pop — иначе ring_head уйдёт в минус.
 * Счётчик overflow_depth зеркально учитывает «пропущенные» start'ы, чтобы
 * End мог правильно декрементировать его вместо ring_head.
 */
static int pgs_ring_overflow_depth = 0;

/*
 * PgsRingSubxactSnap — снимок состояния кольца на момент начала субтранзакции.
 *
 * Зачем нужны снимки?
 * Когда PL/pgSQL-блок BEGIN ... EXCEPTION WHEN OTHERS ... END перехватывает
 * ошибку, PostgreSQL откатывает внутреннюю субтранзакцию через longjmp,
 * минуя standard_ExecutorEnd. Фреймы в кольце «зависают». SubXactCallback
 * при событии ABORT_SUB восстанавливает ring_head из снимка, сделанного
 * при старте субтранзакции, — этим одним присваиванием отсекаются все
 * «зависшие» фреймы внутри отменённого блока.
 *
 * Почему LIFO-стек, а не одна переменная-снимок?
 * При нескольких уровнях вложенных SAVEPOINT каждый вложенный уровень должен
 * иметь свой снимок. Одна переменная перезаписывалась бы на каждом START_SUB
 * и при откате внутреннего уровня теряла бы снимок внешнего.
 *
 * Объём: 256 × (4 + 4) байт = 2048 байт в BSS.
 */
typedef struct PgsRingSubxactSnap
{
    SubTransactionId subxid;     /* идентификатор субтранзакции */
    int              ring_head;  /* значение ring_head на момент START_SUB */
} PgsRingSubxactSnap;

static PgsRingSubxactSnap pgs_ring_subxact_snap[MAX_SUBXACT_STACK_DEPTH];
static int                pgs_ring_subxact_top = 0;

/*
 * Переменные для цепочки хуков.
 *
 * prev_*    — хук, который стоял до нас (может быть NULL, если мы первые).
 *             Используется только в _PG_fini для безопасного восстановления.
 * chained_* — «готовый» указатель для вызова downstream: prev_* если он есть,
 *             иначе standard_ExecutorStart/End.
 *
 * Почему pre-bake chained_*, а не проверять prev_* на каждом вызове?
 * Проверка `if (prev != NULL) prev(...) else standard(...)` — это лишняя
 * ветка на горячем пути при каждом запросе. Один раз вычислив chained_*
 * при загрузке расширения, горячий путь делает один unconditional indirect call.
 */
static ExecutorStart_hook_type prev_ExecutorStart    = NULL;
static ExecutorStart_hook_type chained_ExecutorStart = NULL;
static ExecutorEnd_hook_type   prev_ExecutorEnd      = NULL;
static ExecutorEnd_hook_type   chained_ExecutorEnd   = NULL;


/* ============================================================================
 * Предварительные объявления функций
 * ============================================================================ */

void _PG_init(void);
void _PG_fini(void);

static void pg_query_stack_assign_enabled(bool newval, void *extra);

Datum pg_query_stack(PG_FUNCTION_ARGS);

static void pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_query_stack_ExecutorEnd(QueryDesc *queryDesc);
static void pgs_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
                           SubTransactionId parentSubid, void *arg);
static void pgs_xact_cb(XactEvent event, void *arg);


/* ============================================================================
 * Assign-хук для GUC pg_query_stack.enabled
 * ============================================================================ */

/*
 * Вызывается PostgreSQL при каждом изменении GUC-параметра.
 * Пересчитывает pg_query_stack_active — объединённый флаг горячего пути.
 * IsParallelWorker() не меняется за время жизни процесса, поэтому
 * пересчитывать его при каждом GUC-изменении достаточно и безопасно.
 */
static void
pg_query_stack_assign_enabled(bool newval, void *extra)
{
    pg_query_stack_enabled = newval;
    pg_query_stack_active  = newval && !IsParallelWorker();
}


/* ============================================================================
 * Инициализация и выгрузка расширения
 * ============================================================================ */

void
_PG_init(void)
{
    DefineCustomBoolVariable(
        "pg_query_stack.enabled",
        "Включить или отключить трекинг стека запросов",
        "При отключении накладные расходы близки к нулю. "
        "Можно менять на уровне сеанса без перезапуска сервера.",
        &pg_query_stack_enabled,
        true,
        PGC_USERSET,
        0,
        NULL,                               /* check_hook — проверка не нужна */
        pg_query_stack_assign_enabled,      /* assign_hook — пересчёт active */
        NULL                                /* show_hook */
    );

    pg_query_stack_active = pg_query_stack_enabled && !IsParallelWorker();

    /*
     * Сохраняем текущие хуки до регистрации наших (они могут быть уже заняты
     * другим расширением). Затем вычисляем готовый chained_*-указатель —
     * это позволит горячему пути вызывать downstream одним indirect call,
     * без проверки «а есть ли prev?» при каждом запросе.
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

    RegisterSubXactCallback(pgs_subxact_cb, NULL);
    RegisterXactCallback(pgs_xact_cb, NULL);

    /* Убеждаемся, что хуки установлены именно наши (cassert-сборки). */
    Assert(ExecutorStart_hook == pg_query_stack_ExecutorStart);
    Assert(ExecutorEnd_hook   == pg_query_stack_ExecutorEnd);
}


void
_PG_fini(void)
{
    /*
     * Восстанавливаем хуки только через prev_*, а не через chained_*.
     * Почему? chained_* может указывать на standard_ExecutorStart/End —
     * запись standard_* обратно в глобальный хук означала бы потерю хуков
     * всех других расширений, зарегистрированных после нас.
     * Восстановление через prev_* корректно возвращает хук к тому,
     * что было до нашей загрузки.
     */
    if (ExecutorStart_hook == pg_query_stack_ExecutorStart)
        ExecutorStart_hook = prev_ExecutorStart;
    if (ExecutorEnd_hook == pg_query_stack_ExecutorEnd)
        ExecutorEnd_hook = prev_ExecutorEnd;

    chained_ExecutorStart = NULL;
    chained_ExecutorEnd   = NULL;

    UnregisterXactCallback(pgs_xact_cb, NULL);
    UnregisterSubXactCallback(pgs_subxact_cb, NULL);

    pgs_ring_head           = 0;
    pgs_ring_subxact_top    = 0;
    pgs_ring_overflow_depth = 0;
}


/* ============================================================================
 * Хуки горячего пути: минимальный Start (push) + End (pop).
 * Никаких аллокаций в heap, никаких PG_TRY/PG_CATCH —
 * это было бы слишком дорого при каждом запросе.
 * ============================================================================ */

/*
 * Хук ExecutorStart — вызывается PostgreSQL в начале выполнения каждого запроса.
 *
 * Последовательность действий:
 *   1. Проверяем флаг pg_query_stack_active (трекинг включён и не параллельный воркер).
 *   2. Если кольцо заполнено, увеличиваем счётчик overflow и передаём управление дальше.
 *   3. Иначе сохраняем qd в кольцо, увеличиваем head.
 *   4. Передаём управление downstream-хуку ПОСЛЕДНИМ — так вызов pg_query_stack()
 *      из другого ExecutorStart-хука, стоящего после нас, уже видит наш фрейм.
 */
static void
pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    Assert(queryDesc != NULL);                               /* A2: queryDesc обязан быть не NULL */

    if (unlikely(!pg_query_stack_active))
    {
        chained_ExecutorStart(queryDesc, eflags);
        return;
    }

    Assert(pgs_ring_head >= 0);                              /* A2: head не может уйти в минус */
    if (unlikely(pgs_ring_head >= MAX_QUERY_STACK_DEPTH))
    {
        /*
         * Кольцо заполнено. Увеличиваем счётчик overflow, чтобы ExecutorEnd
         * знал: для этого запроса push не произошёл, значит и pop не нужен.
         */
        pgs_ring_overflow_depth++;
        chained_ExecutorStart(queryDesc, eflags);
        return;
    }

    pgs_ring[pgs_ring_head].qd = queryDesc;
    pgs_ring_head++;

    chained_ExecutorStart(queryDesc, eflags);
}

/*
 * Хук ExecutorEnd — вызывается PostgreSQL в конце выполнения каждого запроса.
 *
 * Зеркально симметричен ExecutorStart: то, что Start положил в кольцо,
 * End должен убрать. Без этого хука ring_head рос бы монотонно и через
 * ~100 запросов переполнялся, а дальше шли бы overflow — стек перестал бы
 * отражать реальную картину.
 *
 * Порядок pop — до вызова downstream: так pg_query_stack(), вызванный из
 * downstream ExecutorEnd другого расширения, уже не видит завершившийся запрос.
 * Это намеренное поведение: запрос завершён — его нет в стеке.
 */
static void
pg_query_stack_ExecutorEnd(QueryDesc *queryDesc)
{
    if (unlikely(!pg_query_stack_active))
    {
        chained_ExecutorEnd(queryDesc);
        return;
    }

    /*
     * Если Start пропустил push (был overflow), End тоже пропускает pop —
     * декрементируем счётчик overflow вместо ring_head, иначе head уйдёт
     * ниже реального дна кольца.
     */
    if (unlikely(pgs_ring_overflow_depth > 0))
    {
        pgs_ring_overflow_depth--;
        chained_ExecutorEnd(queryDesc);
        return;
    }

    /*
     * Защитная проверка: head == 0 означает несимметричный pop.
     * В корректном потоке управления этого не должно быть, но лучше
     * пропустить мутацию кольца, чем уйти в head = -1.
     */
    if (unlikely(pgs_ring_head <= 0))
    {
        chained_ExecutorEnd(queryDesc);
        return;
    }

    Assert(pgs_ring_head > 0);                                          /* A1: симметрия push/pop */
    Assert(pgs_ring_head <= MAX_QUERY_STACK_DEPTH);                     /* A3: head в допустимом диапазоне */
    Assert(pgs_ring[pgs_ring_head - 1].qd == queryDesc);                /* A3: верхняя ячейка — именно наш qd */

    pgs_ring_head--;

    chained_ExecutorEnd(queryDesc);
}

/*
 * Колбэк субтранзакций — LIFO-снимок/восстановление ring_head.
 *
 * Зачем этот колбэк нужен?
 * Когда PL/pgSQL EXCEPTION WHEN OTHERS перехватывает ошибку из AFTER-триггера
 * или вложенного SQL, PostgreSQL откатывает субтранзакцию через longjmp,
 * обходя standard_ExecutorEnd. Хук ExecutorEnd для упавшего запроса не
 * вызывается, и его фрейм «зависает» в кольцевом буфере.
 *
 * SubXactCallback с событием SUBXACT_EVENT_ABORT_SUB срабатывает ДО того,
 * как управление перейдёт в EXCEPTION-блок. Восстановление ring_head из
 * снимка, сделанного при START_SUB, одним присваиванием отсекает все
 * «зависшие» фреймы внутри отменённого блока.
 *
 * PostgreSQL гарантирует вызов SubXactCallback в строгом LIFO-порядке.
 * Ассерт A5 проверяет это в cassert-сборках: верхний снимок LIFO-стека
 * при ABORT_SUB всегда должен соответствовать mySubid.
 *
 * Нет аллокаций в heap, нет PG_TRY/PG_CATCH — это колбэк из критического
 * пути обработки ошибок, он должен быть простым и предсказуемым.
 */
static void
pgs_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
               SubTransactionId parentSubid, void *arg)
{
    int i;
    int found_index;

    (void) parentSubid;
    (void) arg;

    switch (event)
    {
        case SUBXACT_EVENT_START_SUB:
            /*
             * Начало субтранзакции (SAVEPOINT или EXCEPTION-блок).
             * Сохраняем снимок: при откате этой субтранзакции мы знаем,
             * к какому ring_head нужно вернуться.
             * Если LIFO-стек уже переполнен — тихо игнорируем; backward scan
             * в ABORT_SUB просто не найдёт этот subxid (found_index = -1)
             * и ничего не сделает.
             */
            if (unlikely(pgs_ring_subxact_top >= MAX_SUBXACT_STACK_DEPTH))
                break;

            pgs_ring_subxact_snap[pgs_ring_subxact_top].subxid    = mySubid;
            pgs_ring_subxact_snap[pgs_ring_subxact_top].ring_head = pgs_ring_head;
            pgs_ring_subxact_top++;
            break;

        case SUBXACT_EVENT_ABORT_SUB:
            /*
             * A5: PostgreSQL гарантирует LIFO — верхний снимок обязан
             * соответствовать текущему mySubid (или стек пуст при overflow-drop).
             */
            Assert(pgs_ring_subxact_top == 0 ||
                   pgs_ring_subxact_snap[pgs_ring_subxact_top - 1].subxid == mySubid);

            /*
             * Ищем снимок для mySubid сверху вниз (backward scan).
             * Defensive scan обрабатывает случай, когда снимок был потерян
             * из-за переполнения LIFO-стека (found_index < 0 → тихий no-op).
             */
            found_index = -1;
            for (i = pgs_ring_subxact_top - 1; i >= 0; i--)
            {
                if (pgs_ring_subxact_snap[i].subxid == mySubid)
                {
                    found_index = i;
                    break;
                }
            }

            if (unlikely(found_index < 0))
                break;  /* снимок был потерян из-за overflow — ничего не делаем */

            /*
             * Восстанавливаем ring_head до значения на момент START_SUB.
             * Все фреймы, добавленные внутри этой субтранзакции, отсекаются
             * одним присваиванием — никакого цикла, никаких pfree.
             */
            pgs_ring_head        = pgs_ring_subxact_snap[found_index].ring_head;
            pgs_ring_subxact_top = found_index;
            break;

        case SUBXACT_EVENT_COMMIT_SUB:
        case SUBXACT_EVENT_PRE_COMMIT_SUB:
            /*
             * Субтранзакция зафиксирована — снимок больше не нужен,
             * удаляем его из LIFO-стека. Но ring_head НЕ восстанавливаем:
             * запросы, выполненные внутри зафиксированной субтранзакции,
             * должны остаться видны в стеке.
             */
            for (i = pgs_ring_subxact_top - 1; i >= 0; i--)
            {
                if (pgs_ring_subxact_snap[i].subxid == mySubid)
                {
                    pgs_ring_subxact_top = i;
                    break;
                }
            }
            break;

        default:
            break;
    }
}

/*
 * Колбэк завершения транзакции — финальный сброс буфера.
 *
 * Зачем нужен, если есть SubXactCallback?
 * SubXactCallback срабатывает только при наличии субтранзакций (SAVEPOINT).
 * Если ошибка произошла в верхнеуровневой транзакции без EXCEPTION-блоков,
 * SubXactCallback ABORT_SUB не вызывается. Этот колбэк гарантирует, что
 * ring_head вернётся в 0 при любом завершении транзакции (COMMIT или ABORT).
 *
 * Почему достаточно просто обнулить три int, а не освобождать память?
 * Кольцо хранит только сырые указатели на QueryDesc — не владеет памятью.
 * QueryDesc принадлежит PostgreSQL и освобождается им же. Нам достаточно
 * забыть об этих указателях — обнуление трёх int атомарно сбрасывает
 * всё состояние к начальному.
 */
static void
pgs_xact_cb(XactEvent event, void *arg)
{
    (void) arg;
    if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT)
        return;
    pgs_ring_head           = 0;
    pgs_ring_subxact_top    = 0;
    pgs_ring_overflow_depth = 0;
}


/* ============================================================================
 * SQL-функция pg_query_stack() — Set-Returning Function (SRF)
 * ============================================================================
 *
 * Читает кольцевой буфер pgs_ring[0..head-1] и возвращает строки
 * (frame_number, query_text) — по одной на каждый активный запрос в стеке.
 *
 * Параметр skip_count задаёт, сколько фреймов пропустить с «верхушки» стека
 * (т.е. с самого вложенного конца). По умолчанию skip_count=1 — скрывает
 * сам вызов pg_query_stack() из результата. pg_self_query() передаёт 2,
 * скрывая дополнительно свою собственную обёртку.
 *
 * Важные принципы реализации:
 *
 * 1. СНИМОК ОДИН РАЗ (snapshot-once).
 *    Значение pgs_ring_head читается ОДИН РАЗ в начале FIRSTCALL и замораживается
 *    в max_emit. Если во время итерации SRF другой запрос изменит ring_head,
 *    это не повлияет на текущий вызов — мы уже зафиксировали границу.
 *
 * 2. ЛЕНИВОЕ КОПИРОВАНИЕ ТЕКСТА (lazy pstrdup).
 *    Текст запроса копируется через pstrdup ТОЛЬКО при вызове pg_query_stack(),
 *    НЕ в момент ExecutorStart. Это делает горячий путь максимально дешёвым:
 *    push = запись одного указателя. Копирование происходит в контексте
 *    multi_call_memory_ctx, который живёт всё время выполнения SRF.
 *
 *    Почему именно multi_call_memory_ctx, а не TopTransactionContext?
 *    TopTransactionContext уничтожается при COMMIT/ROLLBACK, а тексты нужны
 *    только во время одного вызова pg_query_stack(). multi_call_memory_ctx
 *    точно соответствует нужному времени жизни и освобождается автоматически.
 *
 * 3. ТРИ UAF-ЗАЩИТЫ (G1, G2, G3).
 *    Указатели в кольцевом буфере — сырые, не управляемые нами. PostgreSQL
 *    может освободить память QueryDesc или EState независимо от нас. Перед
 *    разыменованием каждого указателя мы проверяем, что он не NULL:
 *
 *    G1: qd != NULL — слот кольца может быть NULL, если SubXactCallback
 *        восстановил ring_head к меньшему значению, но ячейки «выше» снимка
 *        при этом не были записаны этой субтранзакцией.
 *
 *    G2: qd->estate != NULL — standard_ExecutorEnd обнуляет qd->estate до
 *        освобождения памяти QueryDesc. Если estate = NULL, значит исполнитель
 *        уже завершил работу, и обращаться к sourceText через qd небезопасно.
 *
 *    G3: qd->sourceText != NULL — некоторые внутренние пути SPI могут передавать
 *        NULL в поле sourceText. Проверяем явно, чтобы не разыменовать NULL.
 *
 *    При срабатывании любой защиты строка в результате заменяется на
 *    текст-заполнитель. Строка НЕ пропускается — чтобы не нарушить нумерацию
 *    фреймов и row count.
 *
 * frame_number = 0 — самый внешний (top-level) запрос (наиболее старый);
 * frame_number = max_emit-1 — самый вложенный из видимых.
 */

typedef struct PgsWalkerCtx
{
    int    max_emit;
    char **texts;
} PgsWalkerCtx;

PG_FUNCTION_INFO_V1(pg_query_stack);

Datum
pg_query_stack(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int              skip_count;

    /* SQL DEFAULT = 1, что скрывает из результата сам вызов pg_query_stack(). */
    skip_count = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);
    if (skip_count < 0)
        skip_count = 0;
    else if (skip_count > MAX_QUERY_STACK_DEPTH)
        skip_count = MAX_QUERY_STACK_DEPTH;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext   oldcontext;
        TupleDesc       tupdesc;
        PgsWalkerCtx   *wctx;
        int             snapshot_head;
        int             max_emit;
        bool            active_snapshot;
        int             i;

        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * Переключаемся в multi_call_memory_ctx — всё, что нужно сохранить
         * между вызовами PERCALL (tupdesc, wctx, массив texts[]), аллоцируется
         * именно здесь. Этот контекст живёт всё время выполнения SRF.
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(2);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "frame_number", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_text",   TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Читаем флаг активности один раз. Если расширение отключено или
         * это параллельный воркер — возвращаем пустой результат.
         * Ring_head в этом случае может содержать только частично заполненные
         * данные (Start был no-op, head не увеличивался), поэтому читать
         * кольцо небезопасно.
         */
        active_snapshot = pg_query_stack_active;

        /*
         * Снимок-один-раз: читаем pgs_ring_head ОДИН РАЗ и фиксируем в
         * snapshot_head. Все последующие PERCALL-вызовы используют max_emit,
         * вычисленный из этого снимка. Изменения ring_head в других хуках
         * во время итерации SRF не влияют на этот вызов.
         */
        snapshot_head = active_snapshot ? pgs_ring_head : 0;
        max_emit      = snapshot_head - skip_count;
        if (max_emit < 0)
            max_emit = 0;

        wctx = (PgsWalkerCtx *) palloc(sizeof(PgsWalkerCtx));
        wctx->max_emit = max_emit;
        wctx->texts    = NULL;
        funcctx->user_fctx = wctx;
        funcctx->max_calls = max_emit;

        if (max_emit > 0)
        {
            wctx->texts = (char **) palloc(sizeof(char *) * max_emit);

            /*
             * Прямой перебор: кольцо хранит фреймы в хронологическом порядке
             * вставки, индекс 0 — самый внешний (top-level) запрос,
             * индекс max_emit-1 — самый вложенный из видимых.
             */
            for (i = 0; i < max_emit; i++)
            {
                QueryDesc *qd  = pgs_ring[i].qd;
                EState    *est;
                const char *src;

                Assert(i >= 0 && i < MAX_QUERY_STACK_DEPTH);

                /*
                 * UAF-ЗАЩИТА G1: qd != NULL.
                 * Ячейка кольца может быть NULL, если SubXactCallback ABORT_SUB
                 * восстановил ring_head к меньшему значению, но в ячейки выше
                 * снимка ещё не успели записать qd (их Start не дошёл до этих
                 * слотов). В этом случае слот содержит 0 (BSS-инициализация).
                 */
                if (qd == NULL)
                {
                    wctx->texts[i] = pstrdup("<null queryDesc>");
                    continue;
                }

                /*
                 * UAF-ЗАЩИТА G2: qd->estate != NULL.
                 * Читаем estate ОДИН РАЗ в локальную переменную.
                 * standard_ExecutorEnd обнуляет qd->estate сразу после
                 * завершения исполнителя, ДО освобождения памяти QueryDesc.
                 * NULL здесь означает: исполнитель завершился, но ExecutorEnd
                 * ещё не успел сделать pop из кольца (или не сделает —
                 * например, RAISE внутри триггера обошёл standard_ExecutorEnd,
                 * а SubXactCallback ещё не отработал).
                 * Читать sourceText через qd с estate=NULL — UAF (Use-After-Free).
                 */
                est = qd->estate;
                if (est == NULL)
                {
                    wctx->texts[i] = pstrdup("<estate cleaned up>");
                    continue;
                }

                /*
                 * UAF-ЗАЩИТА G3: qd->sourceText != NULL.
                 * Читаем sourceText ОДИН РАЗ в локальную переменную.
                 * На стандартных путях sourceText жив, пока жив estate (G2).
                 * Тем не менее проверяем явно: некоторые внутренние пути SPI
                 * (prepared-plan executions) могут передавать NULL в sourceText.
                 */
                src = qd->sourceText;
                if (src == NULL)
                {
                    wctx->texts[i] = pstrdup("<null sourceText>");
                    continue;
                }

                /*
                 * Все проверки прошли — копируем текст через pstrdup.
                 * Копия живёт в multi_call_memory_ctx и доступна во всех
                 * последующих PERCALL-вызовах. Сырой указатель src больше
                 * нигде не сохраняется.
                 */
                wctx->texts[i] = pstrdup(src);
            }
        }

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        Datum               values[2];
        bool                nulls[2] = {false, false};
        HeapTuple           tuple;
        PgsWalkerCtx       *wctx = (PgsWalkerCtx *) funcctx->user_fctx;
        int                 idx  = (int) funcctx->call_cntr;
        const char         *qtext;

        qtext = wctx->texts[idx];
        if (qtext == NULL || qtext[0] == '\0')
            qtext = "<unnamed query>";

        values[0] = Int32GetDatum((int32) idx);
        values[1] = CStringGetTextDatum(qtext);

        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        SRF_RETURN_DONE(funcctx);
    }
}
