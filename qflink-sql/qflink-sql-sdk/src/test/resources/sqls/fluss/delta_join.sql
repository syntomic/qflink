SET 'table.optimizer.delta-join.strategy' = 'FORCE';


CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);


CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fact_cdc (
    dim_a BIGINT,
    dim_b BIGINT,
    metric BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a, dim_b) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a',
    -- Flink 2.1 only support append-only source
    -- 'table.merge-engine' = 'first-row',
    -- Flink 2.2 support cdc source
    'table.delete.behavior' = 'ignore'
);

CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.dim_cdc (
    dim_a BIGINT,
    attr BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a',
    'table.delete.behavior' = 'ignore'
);


DROP TABLE IF EXISTS fluss_catalog.fluss.fact_dim_join;
CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fact_dim_join (
    dim_a BIGINT,
    dim_b BIGINT,
    metric BIGINT,
    fact_ts BIGINT,
    attr BIGINT,
    dim_ts BIGINT,
    PRIMARY KEY (dim_a, dim_b) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a'
);


INSERT INTO fluss_catalog.fluss.fact_dim_join
SELECT
    fact_cdc.dim_a,
    fact_cdc.dim_b,
    fact_cdc.metric,
    fact_cdc.ts AS fact_ts,
    dim_cdc.attr,
    dim_cdc.ts AS dim_ts
FROM
	fluss_catalog.fluss.fact_cdc /*+ OPTIONS('scan.startup.mode' = 'earliest') */ fact_cdc
-- ! only support inner join
INNER JOIN
	fluss_catalog.fluss.dim_cdc /*+ OPTIONS('scan.startup.mode' = 'earliest') */ dim_cdc
ON fact_cdc.dim_a = dim_cdc.dim_a
