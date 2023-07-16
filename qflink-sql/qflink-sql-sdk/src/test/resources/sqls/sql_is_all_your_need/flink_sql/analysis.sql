-- make watermark right
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10 s';
-- Split Distinct Aggregation to avoid data skew
-- SET 'table.optimizer.distinct-agg.split.enabled' = 'true';

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

-- ! batch mode can have window result which not triggered by watermark in streaming mode
INSERT INTO dws_metric
SELECT
    DATE_FORMAT(`window_start`, 'yyyy-MM-dd HH:mm:ss') AS `window_start`,
    DATE_FORMAT(`window_end`, 'yyyy-MM-dd HH:mm:ss') AS `window_end`,
    `key_word` AS `dim`,
    COUNT(DISTINCT `key2`) AS `metric`
FROM
    TABLE(TUMBLE(TABLE `dwd_log`, DESCRIPTOR(`rowtime`), INTERVAL '1' MINUTES))
GROUP BY `key_word`, `window_start`, `window_end`;
