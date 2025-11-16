SET 'table.optimizer.delta-join.strategy' = 'FORCE';


CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

CREATE DATABASE IF NOT EXISTS my_db;

USE my_db;


DROP TABLE IF EXISTS `fluss_catalog`.`my_db`.`left_src`;
CREATE TABLE IF NOT EXISTS `fluss_catalog`.`my_db`.`left_src` (
  `city_id` INT NOT NULL,
  `order_id` INT NOT NULL,
  `content` VARCHAR NOT NULL,
  PRIMARY KEY (city_id, `order_id`) NOT ENFORCED
) WITH (
    -- prefix key
    'bucket.key' = 'city_id',
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
);

DROP TABLE IF EXISTS `fluss_catalog`.`my_db`.`right_src`;
CREATE TABLE IF NOT EXISTS `fluss_catalog`.`my_db`.`right_src` (
  `city_id` INT NOT NULL,
  `city_name` VARCHAR NOT NULL,
  PRIMARY KEY (city_id) NOT ENFORCED
) WITH (
    -- in Flink 2.1, delta join only support append-only source
    'table.merge-engine' = 'first_row'
);

DROP TABLE IF EXISTS `fluss_catalog`.`my_db`.`snk`;
CREATE TABLE IF NOT EXISTS `fluss_catalog`.`my_db`.`snk` (
    `city_id` INT NOT NULL,
    `order_id` INT NOT NULL,
    `content` VARCHAR NOT NULL,
    `city_name` VARCHAR NOT NULL,
    PRIMARY KEY (city_id, order_id) NOT ENFORCED
);


EXPLAIN 
INSERT INTO `fluss_catalog`.`my_db`.`snk`
SELECT T1.`city_id`, T1.`order_id`, T1.`content`, T2.`city_name` 
FROM `fluss_catalog`.`my_db`.`left_src` T1
Join `fluss_catalog`.`my_db`.`right_src` T2
ON T1.`city_id` = T2.`city_id`;