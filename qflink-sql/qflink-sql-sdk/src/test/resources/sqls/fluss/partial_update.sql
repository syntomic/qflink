CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

CREATE TEMPORARY TABLE IF NOT EXISTS fluss_catalog.fluss.fact_pk (
    dim_a BIGINT,
    metric BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact_pk.txt',
    'format' = 'json'
);

CREATE TEMPORARY TABLE IF NOT EXISTS fluss_catalog.fluss.dim (
    dim_a BIGINT,
    attr BIGINT,
    ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/dim.txt',
    'format' = 'json'
);


DROP TABLE IF EXISTS fluss_catalog.fluss.fact_pk_dim_join;
CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.fact_pk_dim_join (
    dim_a BIGINT,
    metric BIGINT,
    fact_ts BIGINT,
    attr BIGINT,
    dim_ts BIGINT,
    PRIMARY KEY (dim_a) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_a'
);

-- * just insert, merge on fluss
INSERT INTO fluss_catalog.fluss.fact_pk_dim_join (dim_a, metric, fact_ts)
SELECT
    dim_a,
    metric,
    ts AS fact_ts
FROM
    fluss_catalog.fluss.fact_pk;


-- * just insert, merge on fluss
INSERT INTO fluss_catalog.fluss.fact_pk_dim_join (dim_a, attr, dim_ts)
SELECT
    dim_a,
    attr,
    ts AS dim_ts
FROM
    fluss_catalog.fluss.dim;