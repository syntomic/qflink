CREATE TABLE IF NOT EXISTS `dwd_log` (
    `time` STRING,
    `key_word` STRING,
    `key1` INT,
    `key2` STRING,
    `rowtime` AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(`time`), 0),
    WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '10' SECOND
) WITH (
    'connector' = 'qfile',
    'path' = 'common-data.txt',
    'format' = 'json'
);

CREATE TABLE IF NOT EXISTS `tmp_print` (
    `window_start` STRING,
    `cnt` BIGINT,
    PRIMARY KEY (`window_start`) NOT ENFORCED
) WITH (
    'connector' = 'qprint'
);

INSERT INTO `tmp_print`
SELECT
    DATE_FORMAT(`window_start`, 'yyyy-MM-dd HH:mm:ss') AS `window_start`,
    COUNT(DISTINCT `key2`) as `cnt`
FROM
    TABLE(TUMBLE(TABLE `dwd_log`, DESCRIPTOR(`rowtime`), INTERVAL '1' MINUTES))
GROUP BY `window_start`, `window_end`;