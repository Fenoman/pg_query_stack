/*
 * pg_query_stack.c
 *		Extract information about query stack from current backend
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "access/xact.h"

/*
Ключевое слово PG_MODULE_MAGIC используется для вставки специального "магического" блока информации в скомпилированную библиотеку расширения. 
Этот блок содержит данные о версии PostgreSQL, для которой было скомпилировано расширение, а также другую системную информацию.

Когда PostgreSQL загружает расширение, он проверяет наличие этого магического блока, чтобы убедиться, что расширение совместимо с текущей версией сервера. 
*/
PG_MODULE_MAGIC;

/* 
    Объявление переменной Query_Stack где будет накапливать стек запросов.
    Стек Query_Stack обновляется в хуках ExecutorStart и ExecutorEnd. Он должен корректно восстанавливаться независимо от завершения транзакции.
    При ошибках внутри запросов мы используем блоки PG_TRY и PG_CATCH, чтобы гарантировать, что стек будет корректно обновлён даже при возникновении исключений.
*/
static List *Query_Stack = NIL;

// Структура для хранения копии запроса
typedef struct QueryStackEntry
{
    char *query_text;
} QueryStackEntry;


// Прототипы функций инициализации расширения и выгрузки
void _PG_init(void);
void _PG_fini(void);

// Прототипы хуков и обратных вызовов
static void pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_query_stack_ExecutorEnd(QueryDesc *queryDesc);
static void pg_query_stack_xact_callback(XactEvent event, void *arg);

// Собственный контекст памяти
static MemoryContext QueryStackContext = NULL;


// Прототип нашей функции получения стека запросов
Datum pg_query_stack(PG_FUNCTION_ARGS);

// Сюда сохраняем предыдущие хуки для их восстановления при выгрузке расширения
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

// Собственная реализация функции переворота списка, так как внутренняя list_reverse не доступна для модулей
static List *
pg_list_reverse_copy(List *list)
{
    List       *reversed = NIL;
    ListCell   *cell;

    foreach(cell, list)
    {
        reversed = lcons(lfirst(cell), reversed);
    }

    return reversed;
}

// Удаление последней добавленной записи в стек освобождение памяти
static void
pg_stack_free(void)
{
    if (Query_Stack != NIL)
    {
        QueryStackEntry *entry = (QueryStackEntry *) linitial(Query_Stack);

        if (entry != NULL)
        {
            if (entry->query_text)
                pfree(entry->query_text);
            pfree(entry);
        }

        Query_Stack = list_delete_first(Query_Stack);
    }
}

/* Загрузка расширения в память */
void
_PG_init(void)
{
    // Создаем свой контекст памяти
    QueryStackContext = AllocSetContextCreate(TopTransactionContext,
                                              "QueryStackContext",
                                              ALLOCSET_DEFAULT_SIZES);

    // Регистрируем хуки (сохраняя прошлые)
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = pg_query_stack_ExecutorStart;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = pg_query_stack_ExecutorEnd;
}

/* Выгрузка расширения из памяти */
void
_PG_fini(void)
{
    // Восстанавливаем прошлые хуки
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
Выполняем перехват запроса нашим хуком и записываем его в стек (список Query_Stack). 
Почему именно ExecutorStart:
    ExecutorStart: Вызывается в самом начале выполнения плана запроса, перед тем как будут обработаны какие-либо данные. 
                   Он позволяет выполнить инициализацию или модификации перед началом фактического исполнения запроса, или запись в стек в нашем случае
    ExecutorRun: Вызывается при фактическом выполнении плана, когда началось извлечение кортежей (строк данных). 
                 Он может вызываться несколько раз в течение одного запроса, особенно если запрос выполняется в пакетном режиме или использует курсоры.
             
Именно поэтому нам подходит хук ExecutorStart.
*/
static void
pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    MemoryContext oldcontext;
    
    // Переключаемся на собственный контекст
    oldcontext = MemoryContextSwitchTo(QueryStackContext);

    // Создаём новый элемент стека
    QueryStackEntry *entry = (QueryStackEntry *) palloc(sizeof(QueryStackEntry));

    // Копируем sourceText
    if (queryDesc->sourceText)
        entry->query_text = pstrdup(queryDesc->sourceText);
    else
        entry->query_text = pstrdup("<unnamed query>");

    // Добавляем запись в наш стек
    Query_Stack = lcons(entry, Query_Stack);
    
    // Возвращаемся к предыдущему контексту
    MemoryContextSwitchTo(oldcontext);

    PG_TRY();
    {
        // Далее вызываем следующий хук или стандартную функцию
        if (prev_ExecutorStart)
            prev_ExecutorStart(queryDesc, eflags);
        else
            standard_ExecutorStart(queryDesc, eflags);
    }
    PG_CATCH();
    {
        // Убираем текущий Query_Desc из списка при ошибке и освобождаем память
        pg_stack_free();
        
        // Заново прокидываем ошибку
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* 
    Хук ExecutorEnd. Убираем из стека последний запрос 
*/
static void
pg_query_stack_ExecutorEnd(QueryDesc *queryDesc)
{
    PG_TRY();
    {
        // Сначала вызываем предыдущие хуки 
        if (prev_ExecutorEnd)
            prev_ExecutorEnd(queryDesc);
        else
            standard_ExecutorEnd(queryDesc);
    }
    PG_CATCH();
    {
        // Убираем текущий Query_Desc из списка при ошибке и освобождаем память
        pg_stack_free();
    
        // Заново прокидываем ошибку
        PG_RE_THROW();
    }
    PG_END_TRY();
        
    // Убираем текущий Query_Desc из списка при ошибке и освобождаем память
    pg_stack_free();
}

/*
Это макрос из PostgreSQL, который объявляет информацию о функции для динамического загрузчика PostgreSQL.
Назначение: Сообщает PostgreSQL, что функция pg_query_stack использует версию API V1 и может быть вызвана из SQL.
Итоговый смысл:
    - Без этого объявления PostgreSQL не сможет правильно сопоставить SQL-функцию с C-функцией в динамической библиотеке
    - Обязательно для всех C-функций, экспортируемых в PostgreSQL
*/
PG_FUNCTION_INFO_V1(pg_query_stack);
Datum // Datum — универсальный тип данных в PostgreSQL для хранения любых значений
pg_query_stack(PG_FUNCTION_ARGS) // PG_FUNCTION_ARGS — макрос, который представляет стандартный набор аргументов, передаваемых в функции PostgreSQL на языке C
{
    // Контекст вызова функции, используется для хранения информации между вызовами
    FuncCallContext *funcctx;
    // Получаем параметр _skip_count: это количество запросов в стеке, которые нам необходимо пропустить при возвращении результата
    int              skip_count = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);

    if (skip_count < 0)
        skip_count = 0;

    /* 
        Проверяем, является ли текущий вызов первым в серии вызовов SRF (set-returning function, SRF). 
        Необходимо для инициализации переменных и настройки перед первым возвращением данных.
    */
    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext     oldcontext;
        List             *stack_copy;
        int               depth = 0;
        
        // Инициализируем FuncCallContext для хранения состояния между вызовами
        funcctx = SRF_FIRSTCALL_INIT();

        /* 
            Переключаем контекст памяти на multi_call_memory_ctx, который живет дольше, чем текущий вызов функции.
            Позволяет сохранять данные между вызовами функции.
            В oldcontext сохраняется предыдущий контекст.
            
            Все операции по копированию и созданию списков выполняются в контексте multi_call_memory_ctx
            , который освобождается автоматически после завершения функции. Поэтому утечки тут невозможны.
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* 
            Копируем текущий стек запросов, чтобы он точно не изменился во время исполнения функции 
        */
        if (Query_Stack != NIL)
        {
            stack_copy = NIL;
            ListCell   *lc;
            
            foreach(lc, Query_Stack)
            {
                QueryStackEntry *orig_entry = (QueryStackEntry *) lfirst(lc);
                QueryStackEntry *copy_entry;
        
                // Выделяем память под новый QueryStackEntry в multi_call_memory_ctx
                copy_entry = (QueryStackEntry *) palloc(sizeof(QueryStackEntry));
        
                // Копируем query_text
                if (orig_entry->query_text)
                    copy_entry->query_text = pstrdup(orig_entry->query_text);
                else
                    copy_entry->query_text = pstrdup("<unnamed query>");
        
                // Добавляем копию в наш список
                stack_copy = lappend(stack_copy, copy_entry);
            }
            
            // Пропускаем указанное количество элементов
            while (depth < skip_count && stack_copy != NIL)
            {
                QueryStackEntry *entry = (QueryStackEntry *) linitial(stack_copy);
        
                // Освобождаем память под query_text и структуру, так как они больше не нужны
                pfree(entry->query_text);
                pfree(entry);
                stack_copy = list_delete_first(stack_copy);
                
                depth++;
            }

            /* 
                Переворачиваем список для корректного порядка от 0 (top-level запрос), до N (самый нижний вложенный запрос).
                И cохраняем копию стека для итерации:
                - user_fctx — поле для хранения пользовательских данных между вызовами функции
                ? Копирование необходимо, чтобы обеспечить консистентность данных между вызовами и избежать изменений в оригинальном стеке
            */
            funcctx->user_fctx = pg_list_reverse_copy(stack_copy);
            
            // Получаем количество уровней стека = по сути кол-во вложенных запросов = а также сколько раз функция будет возвращать данные
            funcctx->max_calls = list_length((List *) funcctx->user_fctx);

            /* 
                Создаем описание кортежа при первом вызове
            */
            // Описание кортежа (структура возвращаемых данных) из 2х колонок
            TupleDesc tupdesc = CreateTemplateTupleDesc(2);
            
            /*
                TupleDescInitEntry — инициализируем каждое поле:
                - Первое поле:
                    - (AttrNumber) 1 — номер поля.
                    - "frame_number" — имя поля, уровень вложенности запросов
                    - INT4OID — тип данных (целое число 4 байта).
                - Второе поле:
                    - (AttrNumber) 2 — номер поля.
                    - "query_text" — имя поля, текст перехваченного запроса их стека
                    - TEXTOID — тип данных (текст).
            */
            TupleDescInitEntry(tupdesc, (AttrNumber) 1, "frame_number", INT4OID, -1, 0);
            TupleDescInitEntry(tupdesc, (AttrNumber) 2, "query_text", TEXTOID, -1, 0);
            
            // Завершаем создание описания кортежа, делая его готовым для использования. Благославляем )))
            funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        }
        else
        {
            // Если у нас пустой стек то и делать нечего...
            funcctx->user_fctx = NIL;
            funcctx->max_calls = 0;
        }

        // Восстанавливаем предыдущий контекст памяти
        MemoryContextSwitchTo(oldcontext);
    }

    /*
        Устанавливает funcctx для текущего вызова функции:
        - SRF_PERCALL_SETUP() — макрос, который восстанавливает контекст вызова функции между вызовами
        - Необходимо для правильной работы SRF, чтобы иметь доступ к состоянию между вызовами
    */
    funcctx = SRF_PERCALL_SETUP();
    
    // Получаем наш скопированный стек из контекста
    List       *stack = (List *) funcctx->user_fctx;
    
    if (stack == NIL)
    {
        // Сразу выходим из функции если вдруг стек пустой
        SRF_RETURN_DONE(funcctx);
    }
    
    /*
        Получаем текущий номер вызова (call_cntr)
        ? call_cntr автоматически увеличивается PostgreSQL при каждом вызове функции в SRF. Начинается с 0 (как и массивы/списки в С)
    */
    int         call_cntr = funcctx->call_cntr;

    // Пока текущий номер вызова не достиг крайнего - возвращаем следующую строчку
    if (call_cntr < funcctx->max_calls)
    {
        /*
            Объявление переменных для формирования и возвращения результата
        */
        // Массив значений для полей кортежа
        Datum            values[2];
        // Массив флагов NULL для полей (здесь все значения не NULL)
        bool             nulls[2] = {false, false};
        // Непосредственно сам кортеж (строка) для возвращения
        HeapTuple        tuple;
        
        /* 
            Получаем элемент стека запросов, соответствующий текущему номеру вызова:
            - list_nth(stack, call_cntr); возвращает call_cntr-й элемент списка stack
            - Приводим возвращенный указатель к `QueryStackEntry *`, чтобы работать с его полями
        */
        QueryStackEntry *entry = (QueryStackEntry *) list_nth(stack, call_cntr);
        
        // Уровень вложенности запроса
        int frame_number = call_cntr;
        // Получаем текст запроса
        const char *query_text = entry->query_text;

        // Подстраховка, если вдруг запрос не получен
        if (query_text == NULL || query_text[0] == '\0')
            query_text = "<unnamed query>";

        /*
            Заполняем массив values значениями для текущего кортежа (строки).
            - values[0] — значение для первого поля "frame_number":
                - Возвращаем frame_number
                - Int32GetDatum преобразует int32 в Datum.
            - values[1] — значение для второго поля "query_text":
                - query_text содержит текст текущего запроса.
                - CStringGetTextDatum преобразует C-строку в Datum типа text.
        */
        values[0] = Int32GetDatum(frame_number);
        values[1] = CStringGetTextDatum(query_text);

        /* 
            Создаем кортеж (строку) из описания кортежа и значений полей.
            heap_form_tuple объединяет описание кортежа, значения полей и информацию о NULL в один объект HeapTuple.
        */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

        /*
            Преобразуем HeapTuple в Datum, чтобы вернуть его из функции. HeapTupleGetDatum оборачивает кортеж в Datum.
            ? Функции PostgreSQL на языке C должны возвращать значения типа Datum.
        
            Затем возвращаем текущий результат и указываем, что есть еще данные для возвращения.
            SRF_RETURN_NEXT — макрос, который:
                - Увеличивает счетчик вызовов (call_cntr).
                - Возвращает управление в цикл исполнения PostgreSQL.
                - Обеспечивает корректное поведение SRF, позволяя функции быть вызванной снова для следующего результата.
        */
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    else
    {
        /*
            Когда все результаты уже возвращены (call_cntr >= max_calls):
            - SRF_RETURN_DONE(funcctx); — макрос, который сообщает PostgreSQL, что функция завершила возвращение всех данных.
        */
        SRF_RETURN_DONE(funcctx);
    }
}