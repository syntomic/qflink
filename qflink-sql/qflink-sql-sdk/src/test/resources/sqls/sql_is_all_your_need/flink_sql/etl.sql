SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

CREATE TABLE IF NOT EXISTS `dwd_log` (
    `time` STRING,
    `key_word` STRING,
    `key1` INT,
    `key2` STRING,
    `rowtime` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(`time`), 0),
    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '10' SECOND
) WITH (
    'write-mode' = 'append-only'
);

INSERT INTO `dwd_log`
SELECT
    REGEXP_EXTRACT(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 1) AS `time`,
    REGEXP_EXTRACT(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 2) AS `key_word`,

    JSON_VALUE(REGEXP_EXTRACT(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 3), '$.key1' RETURNING INT) AS `key1`,
    JSON_VALUE(REGEXP_EXTRACT(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 3), '$.key2') AS `key2`
FROM
    `ods_log`;
