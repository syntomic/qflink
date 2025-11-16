CREATE TABLE IF NOT EXISTS `dwd_log` (
    `time` STRING,
    `key_word` STRING,
    `key1` INT,
    `key2` STRING,
    `rowtime` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(`time`), 0),
    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '10' SECOND
) WITH (
    'connector' = 'qfile',
    'path' = 'changelog-data.txt',
    'format' = 'changelog-json',
    'decode.json-parser.enabled' = 'false'
);

CREATE TABLE IF NOT EXISTS `tmp_print` (
    `window_day` STRING,
    `window_hour` STRING,
    `cnt` BIGINT,
    PRIMARY KEY (`window_day`, `window_hour`) NOT ENFORCED
) WITH (
    'connector' = 'qprint'
);


SELECT
    `key2`,
    SUM(`key1`) AS `key1`
FROM
    `dwd_log`
GROUP BY `key2`;




