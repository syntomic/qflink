CREATE TEMPORARY FUNCTION IF NOT EXISTS IgnoreRetractAggregation AS 'cn.syntomic.qflink.sql.udf.ptfs.IgnoreRetractAggregation' LANGUAGE JAVA;

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
    metric,
    ts
FROM
    IgnoreRetractAggregation(
        input => TABLE fact_dep PARTITION BY dim_a,
        uid => 'ignore_retract_agg'
    );





