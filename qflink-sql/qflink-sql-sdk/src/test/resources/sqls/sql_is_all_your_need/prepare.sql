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
    `log` STRING
) WITH (
    'write-mode' = 'append-only'
);

INSERT INTO ods_log
SELECT
    `log`
FROM (VALUES
        ('2023-04-09 15:40:05 stdout {"key1":5,"key2":"val1"}'),
        ('2023-04-09 15:41:05 stdout {"key1":4,"key2":"val2"}'),
        ('2023-04-09 15:42:05 stdout {"key1":1,"key2":"val2"}'),
        ('2023-04-09 15:43:05 stdout {"key1":5,"key2":"val3"}'),
        ('2023-04-09 15:44:05 stdout {"key1":6,"key2":"val4"})')
) AS t (`log`);