-- Tight DO-loop: эмулирует горячий backend выполняющий N тривиальных
-- запросов через executor pipeline. Каждый EXECUTE 'SELECT 1' проходит
-- ExecutorStart/Run/End полностью.
--
-- N передаётся через GUC `bench.iters`: psql-переменные `:iters` НЕ
-- подставляются внутри `DO $$ ... $$` (тело DO — это string literal).
-- Bench-скрипты делают `psql -c "SET bench.iters TO ..." -f ...`.
DO $$
DECLARE
    n int := current_setting('bench.iters')::int;
    i int;
    r int;
    t timestamptz;
BEGIN
    -- warmup
    FOR i IN 1..1000 LOOP r := 1; END LOOP;

    t := clock_timestamp();
    FOR i IN 1..n LOOP
        EXECUTE 'SELECT 1' INTO r;
    END LOOP;
    RAISE NOTICE 'NS_PER_ITER=%',
        (EXTRACT(epoch FROM clock_timestamp()-t)*1e9 / n)::numeric(20,4);
END $$;
