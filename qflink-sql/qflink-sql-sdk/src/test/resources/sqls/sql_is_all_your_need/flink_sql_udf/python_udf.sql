SET 'python.files'='./qflink-python/src/main/python/q-pyflink/udf';
SET 'python.execution-mode'='thread';
SET 'pipeline.operator-chaining' = 'false';
SET 'parallelism.default'='1';

CREATE TEMPORARY FUNCTION IF NOT EXISTS demo_udf AS 'scalar.demo_udf.demo_udf' LANGUAGE PYTHON;

CREATE TABLE IF NOT EXISTS `source` (
    `sepal_length` FLOAT,
    `sepal_width` FLOAT,
    `petal_length` FLOAT,
    `petal_width` FLOAT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.sepal_length.max' = '8',
    'fields.sepal_length.min' = '4',
    'fields.sepal_width.max' = '5',
    'fields.sepal_width.min' = '2',
    'fields.petal_length.max' = '7',
    'fields.petal_length.min' = '1',
    'fields.petal_width.max' = '3',
    'fields.petal_width.min' = '0'
);

CREATE TABLE IF NOT EXISTS `sink` (
    `classifier` INT
) WITH (
    'connector' = 'print'
);


INSERT INTO `sink`
SELECT
    demo_udf(ARRAY[`sepal_length`, `sepal_width`, `petal_length`, `petal_width`]) AS `classifier`
FROM
    `source`;