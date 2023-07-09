-- paimon requires checkpoint interval in streaming mode
SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

DROP TABLE IF EXISTS ods_log;
DROP TABLE IF EXISTS dwd_log;
DROP TABLE IF EXISTS dws_metric;
DROP TABLE IF EXISTS dws_alert;

CREATE TABLE IF NOT EXISTS ods_log (
    log STRING
) WITH (
    'write-mode' = 'append-only'
);

CREATE TEMPORARY TABLE tmp_log (
    log STRING
) WITH (
    'connector' = 'qfile',
    'path' = 'raw-data.txt',
    'format' = 'raw'
);

INSERT INTO ods_log SELECT log FROM tmp_log;