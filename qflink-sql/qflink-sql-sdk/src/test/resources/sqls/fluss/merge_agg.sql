CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

CREATE TEMPORARY TABLE IF NOT EXISTS fact (
    dim_a BIGINT,
    metric BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact.txt',
    'format' = 'json'
);


CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fact_pk (
    dim_a BIGINT,
    metric BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a',
    'table.merge-engine' = 'aggregation',
    'fields.metric.agg' = 'sum',
    -- ! not support retact, only support partial/delete on pk
    'table.delete.behavior' = 'ignore'
);


INSERT INTO fluss_catalog.fluss.fact_pk
SELECT
    dim_a,
    metric,
    ts
FROM
    fact;