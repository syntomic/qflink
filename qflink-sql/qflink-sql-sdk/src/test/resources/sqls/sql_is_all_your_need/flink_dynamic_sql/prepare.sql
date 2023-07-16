SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10 s';

CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

DROP TABLE IF EXISTS `dim_rule`;
CREATE TABLE IF NOT EXISTS `dim_rule` (
    `rule_id` INT COMMENT '规则id',
    `job_id` STRING COMMENT '处理规则作业id',
    `filter` STRING COMMENT '过滤规则',
    `key` STRING COMMENT '分组规则',
    `window` STRING COMMENT '窗口规则',
    `aggregate` ROW<`name` STRING, `input` STRING, `method` STRING> COMMENT '聚合规则',
    `threshold` STRING COMMENT '阈值规则',
    PRIMARY KEY(`rule_id`) NOT ENFORCED
);


INSERT INTO `dim_rule`
SELECT
    *
FROM (
    VALUES
        (1, 'test', 'EXPR$0=="stdout"', 'EXPR$2', 'DAY', ROW('key1_cnt', 'EXPR$1', 'COUNT'), 'key1_cnt>1'),
        (2, 'test', CAST(NULL AS STRING), 'EXPR$0', 'HOUR', ROW('key2_cnt', 'EXPR$2', 'COUNT_DISTINCT'), 'key2_cnt>2')
) t;