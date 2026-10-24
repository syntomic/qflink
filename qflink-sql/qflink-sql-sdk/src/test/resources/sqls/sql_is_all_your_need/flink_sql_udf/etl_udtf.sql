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

-- The regex runs once per log and gives three columns, instead of three REGEXP_EXTRACT calls.
-- ! the json used to be split by the json_tuple of the hive module, which has no release for flink 2.x,
-- the built-in JSON_VALUE reads the fields instead.
INSERT INTO `dwd_log`
SELECT
    `time`,
    `key_word`,
    JSON_VALUE(`json`, '$.key1' RETURNING INT) AS `key1`,
    JSON_VALUE(`json`, '$.key2') AS `key2`
FROM
    `ods_log`, LATERAL TABLE(q_regex_extract(`log`, '(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (.*?) (.*)', 'time', 'key_word', 'json'));
