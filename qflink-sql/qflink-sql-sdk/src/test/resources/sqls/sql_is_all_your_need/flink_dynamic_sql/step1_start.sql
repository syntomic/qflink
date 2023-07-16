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


CREATE TABLE IF NOT EXISTS `dws_rule_alert` (
    `rule_id` INT COMMENT '规则id',
    `window_start` STRING COMMENT '窗口开始时间',
    `key` STRING COMMENT '监控对象',
    `alert_time` STRING COMMENT '报警时间',
    `alert_metrics` MAP<STRING, DOUBLE> COMMENT '报警相关指标'
) WITH (
    'connector' = 'print'
);


INSERT INTO `dws_rule_alert`
SELECT
    1 AS `rule_id`,
    DATE_FORMAT(`time`, 'yyyy-MM-dd 00:00:00') AS `window_start`,
    `key2` AS `key`,
    MAX(`time`) AS `alert_time`,
    MAP['key1', CAST(COUNT(`key1`) AS DOUBLE)] AS `alert_metrics`
FROM
    `dwd_log`
WHERE
    `key_word` = 'stdout'
GROUP BY
    `key2`,
    DATE_FORMAT(`time`, 'yyyy-MM-dd 00:00:00')
HAVING
    COUNT(`key1`) > 1;