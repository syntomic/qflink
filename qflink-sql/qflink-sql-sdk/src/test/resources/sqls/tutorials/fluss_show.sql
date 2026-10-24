-- Print the changelog of a fluss table from the earliest offset, stop it with Ctrl-C:
--   tutorials/run.sh tutorials/fluss_show.sql --table fact_pk
SET 'parallelism.default' = '1';

CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

SELECT * FROM fluss_catalog.fluss.`${table}` /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
