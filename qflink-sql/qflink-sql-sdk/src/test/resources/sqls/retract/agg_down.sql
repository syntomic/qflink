CREATE TABLE IF NOT EXISTS `fact` (
    `dim_a` BIGINT,
    `dim_b` BIGINT,
    `metric` BIGINT,
    `ts` BIGINT,
    PRIMARY KEY (`dim_a`, `dim_b`) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact.txt',
    'format' = 'json'
);


CREATE VIEW fact_dep AS 
SELECT
    dim_a,
    dim_b,
    metric,
    ts
FROM
    (
        SELECT
            dim_a,
            dim_b,
            metric,
            ts,
            ROW_NUMBER() OVER (PARTITION BY dim_a, dim_b ORDER BY ts DESC) AS rn
        FROM
            fact
    ) a
WHERE rn = 1;


SELECT
    dim_a,
    SUM(metric) AS metric,
    LAST_VALUE(ts) AS ts
FROM
    fact_dep
GROUP BY
    dim_a;



