SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

CREATE TABLE IF NOT EXISTS dws_metric (
    `window_start` STRING,
    `window_end` STRING,
    `dim` STRING,
    `metric` BIGINT,
    PRIMARY KEY (`window_start`, `window_end`, `dim`) NOT ENFORCED
);

CREATE TEMPORARY FUNCTION IF NOT EXISTS hll_agg AS 'cn.syntomic.qflink.sql.udf.aggregate.HLLAggregate' LANGUAGE JAVA;
CREATE TEMPORARY FUNCTION IF NOT EXISTS hll_cardinality AS 'cn.syntomic.qflink.sql.udf.scalar.HLLCardinality' LANGUAGE JAVA;

SELECT
    DATE_FORMAT(`min_time`, 'yyyy-MM-dd 00:00:00') AS `window_start`,
    FROM_UNIXTIME(UNIX_TIMESTAMP(`min_time`) + 60) AS `window_end`,
    `key_word` AS `dim`,
    hll_cardinality(hll_agg(min_hll) OVER (PARTITION BY `key_word` ORDER BY `min_time`)) AS `metric`
FROM
    (
        SELECT
            DATE_FORMAT(`time`, 'yyyy-MM-dd HH:mm:00') AS `min_time`,
            `key_word`,
            hll_agg(`key2`) AS min_hll
        FROM
            `dwd_log`
        GROUP BY
            DATE_FORMAT(`time`, 'yyyy-MM-dd HH:mm:00'),
            `key_word`
    ) a;