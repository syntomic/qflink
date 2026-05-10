CREATE TABLE IF NOT EXISTS `fact` (
    `dim_a` BIGINT,
    `dim_b` BIGINT,
    `metric` BIGINT,
    `ts` BIGINT,
    PRIMARY KEY (`dim_a`) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact_single.txt',
    'format' = 'json'
);

CREATE TABLE IF NOT EXISTS `dim` (
    `dim_a` BIGINT,
    `attr` BIGINT,
    `ts` BIGINT,
    PRIMARY KEY (`dim_a`) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/dim.txt',
    'format' = 'json',
    'scan-pause' = '1s'
);

CREATE TABLE IF NOT EXISTS `sink` (
    `dim_a` BIGINT,
    `dim_b` BIGINT,
    `metric` BIGINT,
    `fact_ts` BIGINT,
    `attr` BIGINT,
    `dim_ts` BIGINT,
    PRIMARY KEY (`dim_a`) NOT ENFORCED
) WITH (
    'connector' = 'qprint'
);


CREATE VIEW dim_dep AS 
SELECT
    dim_a,
    attr,
    ts
FROM
    (
        SELECT
            dim_a,
            attr,
            ts,
            ROW_NUMBER() OVER (PARTITION BY dim_a ORDER BY ts DESC) AS rn
        FROM
            dim
    ) a
WHERE rn = 1;

INSERT INTO sink
SELECT
    fact.dim_a,
    fact.dim_b,
    fact.metric,
    fact.ts AS fact_ts,
    dim_dep.attr,
    dim_dep.ts AS dim_ts
FROM
    fact
LEFT JOIN
    dim_dep
ON fact.dim_a = dim_dep.dim_a;