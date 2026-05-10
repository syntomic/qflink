CREATE TABLE IF NOT EXISTS `fact` (
    `dim_a` BIGINT,
    `metric` BIGINT,
    `attr` BIGINT,
    `ts` BIGINT,
    PRIMARY KEY (`dim_a`) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact_join.txt',
    'format' = 'json'
);


CREATE VIEW fact_dep AS 
SELECT
    dim_a,
    metric,
    attr,
    ts
FROM
    (
        SELECT
            dim_a,
            metric,
            attr,
            ts,
            ROW_NUMBER() OVER (PARTITION BY dim_a ORDER BY ts DESC) AS rn
        FROM
            fact
    ) a
WHERE rn = 1;


SELECT
    attr,
    SUM(metric) AS metric,
    LAST_VALUE(ts) AS ts
FROM
    fact_dep
GROUP BY
    attr;



