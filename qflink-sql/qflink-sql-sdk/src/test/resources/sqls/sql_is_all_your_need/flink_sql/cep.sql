SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

SELECT *
FROM dwd_log
    MATCH_RECOGNIZE (
        PARTITION BY key_word
        ORDER BY rowtime
        MEASURES
            START_ROW.rowtime AS start_ts,
            LAST(VAL_DOWN.rowtime) AS bottom_ts,
            LAST(VAL_UP.rowtime) AS end_ts
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO LAST VAL_UP
        PATTERN (START_ROW VAL_DOWN+ VAL_UP)
        DEFINE
            VAL_DOWN AS
                (LAST(VAL_DOWN.key1, 1) IS NULL AND VAL_DOWN.key1 < START_ROW.key1) OR
                    VAL_DOWN.key1 < LAST(VAL_DOWN.key1, 1),
            VAL_UP AS
                VAL_UP.key1 > LAST(VAL_DOWN.key1, 1)
    ) MR;
