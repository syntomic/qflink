CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fact_lake (
    dim_a BIGINT,
    dim_b BIGINT,
    metric BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a, dim_b) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '10s'
);


SELECT
    dim_a,
    SUM(metric) AS metric
FROM
    -- * first read paimon snapshots, then read fluss
    -- fluss_catalog.fluss.fact_lake
    -- ! flink 2.2 cannot read by $lake, use flink 1.20 to read only paimon lake data.
    -- fluss_catalog.fluss.fact_lake$lake
GROUP BY dim_a;