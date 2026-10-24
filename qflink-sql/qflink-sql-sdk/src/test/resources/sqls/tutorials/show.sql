-- Print a paimon table of the tutorials, run it in batch mode:
--   tutorials/run.sh --batch tutorials/show.sql --table dws_metric
-- ! a batch SELECT * fails on a rowtime attribute, list the other columns of such a table:
--   tutorials/run.sh --batch tutorials/show.sql --table dwd_log --columns 'time, key_word, key1, key2'
CREATE CATALOG my_catalog WITH (
    'type'='paimon',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

SELECT ${columns:-*} FROM `${table}`;
