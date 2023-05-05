SET 'execution.checkpointing.interval' = '10 s';
SET 'table.exec.state.ttl' = '2 d';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

CREATE TABLE IF NOT EXISTS `dws_alert` (
    `window_start` STRING,
    `key` STRING,
    `time` STRING,
    `metric` BIGINT,
    PRIMARY KEY (`window_start`, `key`) NOT ENFORCED
);

-- INSERT INTO `dws_alert`
SELECT
    DATE_FORMAT(`time`, 'yyyyMMdd') AS `window_start`,
    `key_word` AS `key`,
    MAX(`time`) AS `time`,
    SUM(`key1`) AS `metric`
FROM
    `dwd_log`
GROUP BY
    DATE_FORMAT(`time`, 'yyyyMMdd'), `key_word`
HAVING SUM(`key1`) > 10;