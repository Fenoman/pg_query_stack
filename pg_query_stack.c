/*
 * pg_query_stack.c
 *		Extract information about query stack from current backend
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "access/xact.h"

/*
Ключевое слово PG_MODULE_MAGIC используется для вставки специального "магического" блока информации в скомпилированную библиотеку расширения. 
Этот блок содержит данные о версии PostgreSQL, для которой было скомпилировано расширение, а также другую системную информацию.

Когда PostgreSQL загружает расширение, он проверяет наличие этого магического блока, чтобы убедиться, что расширение совместимо с текущей версией сервера. 
*/
PG_MODULE_MAGIC;

// Прототипы функций
void _PG_init(void);
void _PG_fini(void);
static void pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_query_stack_ExecutorFinish(QueryDesc *queryDesc);

// Сюда сохраняем предыдущие хуки для их восстановления при выгрузке расширения
static ExecutorStart_hook_type  prev_ExecutorStart  = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;

// Структура для хранения перехваченного запрос
typedef struct QueryStackEntry
{
    char       *query;
} QueryStackEntry;

// Список структур QueryStackEntry. Хранит тексты запросов в виде стека LIFO, длина списка означает глубину вложенности.
static List *query_stack = NIL;

/*
    Список query_stack и его элементы выделяются в контексте памяти по умолчанию. 
    Для обеспечения автоматического освобождения памяти в случае ошибок лучше связать "выделения" с определенным контекстом памяти.
    Таким образом, при возникновении ошибки механизмы контекста памяти PostgreSQL будут автоматически очищать выделенную память.
*/
static MemoryContext pg_query_stack_context = NULL;


/*
Выполняем перехват запроса нашим хуком и записываем его в наш стек (список query_stack). 
Почему именно ExecutorStart:
    ExecutorStart: Вызывается в самом начале выполнения плана запроса, перед тем как будут обработаны какие-либо данные. 
                   Он позволяет выполнить инициализацию или модификации перед началом фактического исполнения запроса, или запись в стек в нашем случае
    ExecutorRun: Вызывается при фактическом выполнении плана, когда началось извлечение кортежей (строк данных). 
                 Он может вызываться несколько раз в течение одного запроса, особенно если запрос выполняется в пакетном режиме или использует курсоры.
             
Именно поэтому нам подходит хук ExecutorStart. Остальные даже не рассматривал.
*/
static void
pg_query_stack_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    // Проверяем, а есть ли текст запроса?
    if (queryDesc->sourceText)
    {
        /* 
            Переключаемся на контекст pg_query_stack_context, сохраняя старый в oldcontext.
            Это нужно, чтобы данные сохранялись в течении всех возможных вызовов данного хука на всех уровнях фреймов
        */
        MemoryContext oldcontext = MemoryContextSwitchTo(pg_query_stack_context);
        /*  
            - выясняем размер(sizeof) структуры QueryStackEntry 
            - выделяем под нее память palloc (postgres allocation)
            - сохраняем указатель в переменную entry
            ? Память, выделенная с помощью palloc (вместо malloc), автоматически управляется контекстами памяти PostgreSQL, 
               что облегчает очистку и предотвращает утечки памяти.
        */
        QueryStackEntry *entry = palloc(sizeof(QueryStackEntry));
        /*  
            - создаем копию строки, используя palloc (pstrdup все делаем сам) для выделения памяти под новую строку
            - сохраняем копию в query
            ? pstrdup(вместо strdup) гарантирует, что память под строку будет освобождена вместе с контекстом памяти, к которому она принадлежит
        */
        entry->query = pstrdup(queryDesc->sourceText);
        /*  
            - добавляем объект entry в начало списка query_stack (будет создан автоматически если NIL)
            ? lcons — это функция из API списков PostgreSQL, используемая для добавления элемента в НАЧАЛО списка. 
            ? То есть новый элемент будет иметь индекс 0
        */
        query_stack = lcons(entry, query_stack);
        
        // Переключаемся на старый контекст памяти сохраненный в oldcontext
        MemoryContextSwitchTo(oldcontext);
    }

    // Обертываем дальнейшее выполнение в PG_TRY/PG_CATCH
    PG_TRY();
    {
        // Вызываем предыдущий хук, если он есть
        if (prev_ExecutorStart)
            prev_ExecutorStart(queryDesc, eflags);
        else
            // Либо вызываем стандартную функцию для продолжения обычного процесса выполнения запроса
            standard_ExecutorStart(queryDesc, eflags);
    }
    PG_CATCH();
    {
        // Сохраняем текущую информацию об ошибке, которая была перехвачена. Она понадобится нам в конце!
        ErrorData *errdata = CopyErrorData();
        /*
            Сбрасываем внутреннюю информацию об ошибке, что позволяет продолжить выполнение без наличия активной ошибки.
            ? Если не вызвать FlushErrorState(), то при попытке выполнить дополнительные действия (вызвать pg_query_stack_ExecutorFinish) 
              СУБД будет считать, что все еще есть необработанное исключение, и может вести себя непредсказуемо.
        */
        FlushErrorState();

        // Гарантируем вызов ExecutorFinish для очистки
        PG_TRY();
        {
            pg_query_stack_ExecutorFinish(queryDesc);
        }
        PG_CATCH();
        {
            // Игнорируем ошибки внутри ExecutorFinish
            FlushErrorState();
        }
        PG_END_TRY();
        
        // Повторно выбрасываем исходную ошибку, как если бы она произошла без вмешательства, сохраняя при этом оригинальное сообщение об ошибке
        ReThrowError(errdata);
        
        /*
            !!!
            Все это позволяет гарантировать, что pg_query_stack_ExecutorFinish всегда будет вызвана, даже при возникновении ошибок во время выполнения запроса. 
            Это поможет избежать утечки памяти такого вида.
        */
    }
    PG_END_TRY();
}


/*
    Удаляем текст запроса из нашего стека (query_stack)
*/
static void
pg_query_stack_ExecutorFinish(QueryDesc *queryDesc)
{
    // Удаляем текущий запрос из стека
    if (query_stack != NIL)
    {
        /*
            !!! Переключение контекста на pg_query_stack_context не требуется !!!!
            Поскольку мы не выделяем новую память, а лишь освобождаем существующие ресурсы, то все операции выполняются корректно в текущем контексте памяти.
            Хотя и страшного ничего не произойдет если это все же сделать.
        */
    
        /*
            * извлекаем первый элемент из `query_stack` с помощью функции `linitial`, которая возвращает первый элемент списка 
            * приводим его к типу `QueryStackEntry *`
            ? функция `linitial` из API PostgreSQL. Возвращает указатель типа `void *`
        */
        QueryStackEntry *entry = (QueryStackEntry *) linitial(query_stack);
        /*
            Освобождаем память, выделенную под копию строки с текстом запроса, сохраненную в `entry->query`
        */
        pfree(entry->query);
        /*
            Освобождаем память, выделенную под саму структуру `QueryStackEntry`
            ? Раздельное выделение памяти:
                - Память для структуры `QueryStackEntry` (palloc) и для строки `entry->query` (pstrdup) выделялись отдельно
                - Следовательно, их нужно освобождать по отдельности, чтобы избежать утечек памяти

        */
        pfree(entry);
        /*
            - удаляем первый элемент из списка `query_stack`, обновляя указатель на список
            - после этого вершиной стека становится следующий элемент (LIFO)
        */
        query_stack = list_delete_first(query_stack);
    }
    
    // Вызываем предыдущий хук или стандартную функцию ExecutorFinish
    if (prev_ExecutorFinish)
        prev_ExecutorFinish(queryDesc);
    else
        standard_ExecutorFinish(queryDesc);
}


/*
    Если во время выполнения запроса между ExecutorStart и ExecutorFinish произойдет ошибка, функция pg_query_stack_ExecutorFinish может не быть вызвана. 
    Это может произойти, если запрос упал из-за исключения. 
    В результате в стеке query_stack не будет удалена последняя запись, что приведет к неконсистентному состоянию стека и потенциальным утечкам памяти.
*/
static void
pg_query_stack_xact_callback(XactEvent event, void *arg)
{
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
    {
        // Просто сбрасываем контекст и вся память будет освобождена
        MemoryContextReset(pg_query_stack_context);
        query_stack = NIL;
    }
}


/*
    В PostgreSQL транзакции могут содержать под-транзакции, создаваемые с помощью сохранённых точек (SAVEPOINT). 
    Если внутри такой под-транзакции возникает ошибка, и выполняется откат к сохранённой точке (ROLLBACK TO SAVEPOINT),
    состояние памяти и данные, выделенные в рамках этой под-транзакции, должны быть корректно очищены.
*/
static void
pg_query_stack_subxact_callback(SubXactEvent event, SubTransactionId subId, SubTransactionId parentSubId, void *arg)
{
    /*
        Если произошел откат под-транзакции, необходимо очистить контекст памяти и стек запросов,
        чтобы избежать утечек памяти и неконсистентного состояния.
    */
    if (event == SUBXACT_EVENT_ABORT_SUB)
    {
        MemoryContextReset(pg_query_stack_context);
        query_stack = NIL;
    }
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
pg_query_stack(PG_FUNCTION_ARGS)  // PG_FUNCTION_ARGS — макрос, который представляет стандартный набор аргументов, передаваемых в функции PostgreSQL на языке C
{
    /* 
        Получаем параметр _skip_count: это количество запросов в стеке, которые нам необходимо пропустить при возвращении результата
    */
    int skip_count = PG_ARGISNULL(0) ? 0 : PG_GETARG_INT32(0);
    
    /*
        Набор переменных, необходимых для реализации функции, возвращающей набор (set-returning function, SRF)
    */
    // Контекст вызова функции, используется для хранения информации между вызовами
    FuncCallContext *funcctx;
    // Счетчик вызовов функции, указывает, сколько раз функция была вызвана в текущем контексте
    int             call_cntr;
    // Общее количество вызовов, определяет, сколько раз функция должна вернуть результат
    int             max_calls;


    /* 
        Проверяем, является ли текущий вызов первым в серии вызовов SRF. 
        Необходимо для инициализации переменных и настройки перед первым возвращением данных.
    */
    if (SRF_IS_FIRSTCALL()) 
    {
        MemoryContext   oldcontext;

        // Инициализируем FuncCallContext для хранения состояния между вызовами
        funcctx = SRF_FIRSTCALL_INIT();

        /* 
            Переключаем контекст памяти на multi_call_memory_ctx, который живет дольше, чем текущий вызов функции.
            Позволяет сохранять данные между вызовами функции.
            В oldcontext сохраняется предыдущий контекст.
        */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        
        /*
            Делаем "глубокую" копию стека вместе со структурой QueryStackEntry
            ? Если мы сделаем копию query_stack с помощью list_copy(...) и сохраним его в funcctx->user_fctx, то это создаст только неглубокую копию списка, 
            ? копируя узлы списка, но не структуру QueryStackEntry, на которые они указывают. 
            ? В результате и query_stack, и funcctx->user_fctx будут содержать указатели на одни и те же структуры QueryStackEntry.
        */
        List       *stack_copy = NIL;
        ListCell   *lc;
        
        int idx = 0;
        foreach(lc, query_stack)
        {
            // Пропускаем нужное количество запросов в стеке
            if (idx < skip_count)
            {
                idx++;
                continue;
            }
            
            QueryStackEntry *entry = (QueryStackEntry *) lfirst(lc);
            /*
                Может возникнуть вопрос, а почему тут мы используем выделение памяти без переключения на контекст pg_query_stack_context?
                
                Это намеренно и уместно, потому что:
                1. Когда выполняется функция PostgreSQL, она имеет свой собственный контекст памяти (обычно дочерний контекст per-query или per-plan). 
                   "Выделения", сделанные в этом контексте, автоматически освобождаются по завершению  функции.
                2. Копии, которые мы создаем для записей стека запросов, нужны только в функции pg_query_stack. 
                   Они используются для построения результата, который вернет функция. 
                   После возвращения функции нет необходимости сохранять эти копии.
                3. Выделяя память в "контексте памяти функции", мы гарантируем, что вся она будет должным образом очищена без дополнительных усилий. 
                   Риск утечки памяти отсутствует, поскольку очисткой занимается система контекста памяти PostgreSQL.
                
                Итого: Переключение на pg_query_stack_context внутри pg_query_stack привело бы к разрастанию памяти, 
                       поскольку этот контекст не сбрасывается до выхода из бэкенда или выгрузки расширения.
            */
            QueryStackEntry *new_entry = palloc(sizeof(QueryStackEntry));
            new_entry->query = pstrdup(entry->query);
            stack_copy = lappend(stack_copy, new_entry);
            
            idx++;
        }
        
        // Получаем количество уровней стека = по сути кол-во вложенных запросов = а также сколько раз функция будет возвращать данные
        funcctx->max_calls = list_length(stack_copy);
        
        /* 
            Сохраняем копию стека для итерации:
            - user_fctx — поле для хранения пользовательских данных между вызовами функции
            ? Копирование необходимо, чтобы обеспечить консистентность данных между вызовами и избежать изменений в оригинальном стеке
        */
        funcctx->user_fctx = stack_copy;

        /* 
            Создаем описание кортежа при первом вызове.
            Проверяем, инициализировано ли описание кортежа (tuple_desc).
            Если tuple_desc == NULL, значит, оно еще не создано, и необходимо создать его, на самом деле просто подстраховка
        */
        if (funcctx->tuple_desc == NULL)
        {
            // Описание кортежа (структура возвращаемых данных)
            TupleDesc   tupdesc;
            
            // Создаем шаблон описания кортежа с 2 полями
            tupdesc = CreateTemplateTupleDesc(2);
            
            /*
                TupleDescInitEntry — инициализируем каждое поле:
                - Первое поле:
                    - (AttrNumber) 1 — номер поля.
                    - "frame_number" — имя поля, уровень вложенности запросос
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
        
        // Восстанавливаем предыдущий контекст памяти
        MemoryContextSwitchTo(oldcontext);
    }
    
    /*
        Устанавливает funcctx для текущего вызова функции:
        - SRF_PERCALL_SETUP() — макрос, который восстанавливает контекст вызова функции между вызовами
        - Необходимо для правильной работы SRF, чтобы иметь доступ к состоянию между вызовами
    */
    funcctx = SRF_PERCALL_SETUP();

    /*
        Получаем текущий номер вызова (call_cntr) и общее количество вызовов (max_calls).
        ? call_cntr автоматически увеличивается PostgreSQL при каждом вызове функции в SRF. Начинается с 0 (как и массивы/списки в С)
    */
    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;

    if (call_cntr < max_calls)
    {
        /*
            Объявление переменных для формирования и возвращения результата
        */
        // Будет содержать итоговый результат для возвращения
        Datum       result;
        // Непосредственно сам кортеж (строка) для возвращения
        HeapTuple   tuple;
        // Массив значений для полей кортежа
        Datum       values[2];
        // Массив флагов NULL для полей (здесь все значения не NULL)
        bool        nulls[2] = {false, false};
        
        /*
            Объявление переменных для извлечения данных из стека
        */
        // Получение копии стека запросов из контекста функции
        List       *stack = (List *) funcctx->user_fctx;
        // Переменная для текущего элемента стека
        QueryStackEntry *entry;

        /* 
            Получаем элемент стека запросов, соответствующий текущему номеру вызова:
            - list_nth(stack, call_cntr); возвращает call_cntr-й элемент списка stack
            - Приводим возвращенный указатель к `QueryStackEntry *`, чтобы работать с его полями
        */
        entry = (QueryStackEntry *) list_nth(stack, call_cntr);

        /*
            Заполняем массив values значениями для текущего кортежа (строки).
            - values[0] — значение для первого поля "frame_number":
                - Вычисляем номер кадра стека как max_calls - call_cntr для нумерации от внешнего запроса к внутреннему.
                - Int32GetDatum преобразует int32 в Datum.
            - values[1] — значение для второго поля "query_text":
                - entry->query содержит текст текущего запроса.
                - CStringGetTextDatum преобразует C-строку в Datum типа text.
        */
        values[0] = Int32GetDatum(max_calls - call_cntr - 1);
        values[1] = CStringGetTextDatum(entry->query);

        /* 
            Создаем кортеж (строку) из описания кортежа и значений полей.
            heap_form_tuple объединяет описание кортежа, значения полей и информацию о NULL в один объект HeapTuple.
        */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        /*
            Преобразуем HeapTuple в Datum, чтобы вернуть его из функции. 
            HeapTupleGetDatum оборачивает кортеж в Datum.
            ? Функции PostgreSQL на языке C должны возвращать значения типа Datum.
        */
        result = HeapTupleGetDatum(tuple);

        /*
            Возвращаем текущий результат и указываем, что есть еще данные для возвращения.
            SRF_RETURN_NEXT — макрос, который:
                - Увеличивает счетчик вызовов (call_cntr).
                - Возвращает управление в цикл исполнения PostgreSQL.
                - Обеспечивает корректное поведение SRF, позволяя функции быть вызванной снова для следующего результата.
        */
        SRF_RETURN_NEXT(funcctx, result);
    }
    else
    {
        /*
            Когда все результаты уже возвращены (call_cntr >= max_calls):
            - "Освобождаем" память занятую копией
            - SRF_RETURN_DONE(funcctx); — макрос, который сообщает PostgreSQL, что функция завершила возвращение всех данных.
        */
        List       *stack_copy = (List *) funcctx->user_fctx;
        ListCell   *lc;

        foreach(lc, stack_copy)
        {
            QueryStackEntry *entry = (QueryStackEntry *) lfirst(lc);
            pfree(entry->query);
            pfree(entry);
        }
        list_free(stack_copy);

        SRF_RETURN_DONE(funcctx);
    }
}


/* 
    Вызывается автоматически при загрузке модуля 
*/
void
_PG_init(void)
{
    // Создаем изолированный контекст памяти
    pg_query_stack_context = AllocSetContextCreate(TopMemoryContext, "pg_query_stack_context", ALLOCSET_DEFAULT_SIZES);
    
    // Используем  ExecutorStart для перехвата запроса
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = pg_query_stack_ExecutorStart;

    // Используем ExecutorFinish для очистки стека
    prev_ExecutorFinish = ExecutorFinish_hook;
    ExecutorFinish_hook = pg_query_stack_ExecutorFinish;
    
    // Регистрируем собственный перехватчик отката или коммита транзакции, чтобы избежать утечек памяти
    RegisterXactCallback(pg_query_stack_xact_callback, NULL);
    
    // Регистрация колбэка под-транзакции, чтобы избежать утечек памяти в случае использования SAVEPOINTS
    RegisterSubXactCallback(pg_query_stack_subxact_callback, NULL);
}


/* 
    Вызывается при выгрузке модуля (например, при остановке сервера или удалении расширения). 
    Позволяет выполнить необходимые действия по очистке. 
*/
void
_PG_fini(void)
{
    /*
        Обязательно восстанавливаем хуки.
        Так мы не нарушаем цепочку вызовов и обеспечиваем совместимость с другими расширениями, которые могут использовать те же хуки. 
        Сохраняя предыдущее значение и вызывая его внутри нашего хука (если это необходимо), мы позволяем другим модулям работать корректно.
    */
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorFinish_hook = prev_ExecutorFinish;
    
    // Отвязываемся от событий транзакций
    UnregisterXactCallback(pg_query_stack_xact_callback, NULL);
    
    // Отвязка от колбэка под-транзакции
    UnregisterSubXactCallback(pg_query_stack_subxact_callback, NULL);
    
    // Освобождаем созданный контекст памяти
    if (pg_query_stack_context)
        MemoryContextDelete(pg_query_stack_context);
}