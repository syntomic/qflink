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

CREATE TEMPORARY FUNCTION IF NOT EXISTS q_regex_extract AS 'cn.syntomic.qflink.sql.udf.table.QRegexExtract' LANGUAGE JAVA;
-- load hive functions
LOAD MODULE hive WITH ('hive-version' = '3.1.3');

INSERT INTO `dwd_log`
SELECT
    `time`,
    `key_word`,
    -- json_tuple return string type
    CAST(`key1` AS INT) AS `key1`,
    `key2`
FROM
    (
        SELECT
            `time`,
            `key_word`,
            `json`
        FROM
            `ods_log`, LATERAL TABLE(q_regex_extract(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 'time', 'key_word', 'json'))
    ) a, LATERAL TABLE(json_tuple(`json`, 'key1', 'key2')) AS b(`key1`, `key2`);
