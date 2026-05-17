CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);


CREATE CATALOG paimon_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);


-- for streaming lakehouse
DROP TABLE IF EXISTS fluss_catalog.fluss.fact_lake;
DROP TABLE IF EXISTS paimon_catalog.fluss.fact_lake;
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


-- for delta join
DROP TABLE IF EXISTS fluss_catalog.fluss.fact_cdc;
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

DROP TABLE IF EXISTS fluss_catalog.fluss.dim_cdc;
CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.dim_cdc (
    dim_a BIGINT,
    attr BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a',
    'table.delete.behavior' = 'ignore'
);


-- example data
INSERT INTO fluss_catalog.fluss.fact_lake VALUES
    (1, 1, 5, 1),
    (2, 300000, 2, 2),
    (1, 1, 8, 3);

INSERT INTO fluss_catalog.fluss.fact_cdc VALUES
    (1, 1, 5, 1),
    (2, 300000, 2, 2),
    (1, 1, 8, 3);


INSERT INTO fluss_catalog.fluss.dim_cdc VALUES
    (1, 1, 1),
    (2, 2, 2),
    (1, 3, 3);
