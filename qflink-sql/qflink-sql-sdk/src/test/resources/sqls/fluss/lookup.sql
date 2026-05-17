CREATE CATALOG fluss_catalog 
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

CREATE TEMPORARY TABLE IF NOT EXISTS fact (
    dim_a BIGINT,
    dim_b BIGINT,
    metric BIGINT,
    ts BIGINT,
    ptime AS proctime(),
    PRIMARY KEY (dim_a, dim_b) NOT ENFORCED
) WITH (
    'connector' = 'qfile',
    'path' = 'retract/fact.txt',
    'format' = 'json'
);


CREATE TABLE IF NOT EXISTS fluss_catalog.fluss.dim_mapping (
    dim_b        BIGINT,
    uid_int32    BIGINT,
    PRIMARY KEY (dim_b) NOT ENFORCED
) WITH (
    'bucket.key' = 'dim_b',
    'auto-increment.fields' = 'uid_int32'
);


SELECT
    fact.dim_a,
    fact.dim_b,
    fact.metric,
    fact.ts,
    dim_mapping.uid_int32
FROM fact
LEFT JOIN fluss_catalog.fluss.dim_mapping /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
    FOR SYSTEM_TIME AS OF fact.ptime AS dim_mapping
ON fact.dim_b = dim_mapping.dim_b;