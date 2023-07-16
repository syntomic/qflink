SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

CREATE TEMPORARY TABLE IF NOT EXISTS `dwd_log` (
    `time` STRING,
    `key_word` STRING,
    `key1` INT,
    `key2` STRING,
    `data` AS ROW(`key_word`, `key1`, `key2`) COMMENT '构造数据字段，供规则动态调用',
    `proc_time` AS PROCTIME()
) WITH (
    'connector' = 'qfile',
    'path' = 'common-data.txt',
    'format' = 'json',
    'scan-pause' = '2 s'
);

CREATE TEMPORARY TABLE IF NOT EXISTS `dws_rule_alert` (
    `rule_id` INT COMMENT '规则id',
    `window_start` STRING COMMENT '窗口开始时间',
    `key` STRING COMMENT '监控对象',
    `alert_time` STRING COMMENT '报警时间',
    `alert_metrics` MAP<STRING, DOUBLE> COMMENT '报警相关指标'
) WITH (
    'connector' = 'print'
);


CREATE TEMPORARY FUNCTION IF NOT EXISTS dynamic_key AS 'cn.syntomic.qflink.sql.udf.scalar.DynamicKey' LANGUAGE JAVA;
CREATE TEMPORARY FUNCTION IF NOT EXISTS dynamic_window AS 'cn.syntomic.qflink.sql.udf.scalar.DynamicWindow' LANGUAGE JAVA;
CREATE TEMPORARY FUNCTION IF NOT EXISTS dynamic_filter AS 'cn.syntomic.qflink.sql.udf.scalar.DynamicFilter' LANGUAGE JAVA;
CREATE TEMPORARY FUNCTION IF NOT EXISTS dynamic_agg AS 'cn.syntomic.qflink.sql.udf.aggregate.DynamicAggregate' LANGUAGE JAVA;


INSERT INTO `dws_rule_alert`
SELECT
    `rule_id`,
    `window_start`,
    `key`,
    MAX(`time`) AS `alert_time`,
    dynamic_agg(`data`, `aggregate`) AS `alert_metrics`
FROM
    (
        SELECT
            `rule_id`,
            dynamic_window(`time`, `window`) AS `window_start`,
            dynamic_key(`data`, `key`) AS `key`,
            `aggregate`,
            `threshold`,
            `time`,
            `data`
        FROM
            `dwd_log` AS `log`
        LEFT JOIN
            `dim_rule` AS `rule`
        ON `rule`.`job_id` = 'test'
        WHERE
            dynamic_filter(`data`, `filter`) AND `rule_id` IS NOT NULL
    ) `log_with_rule`
GROUP BY
    `key`, `rule_id`, `window_start`, `threshold`, `aggregate`
HAVING
    dynamic_filter(dynamic_agg(`data`, `aggregate`), `threshold`);