-- Flink + Hudi SQL Example Scripts from Chapter 4

CREATE CATALOG hudi_catalog
WITH (
  'type' = 'hudi',
  'catalog.path' = 'file:///tmp/hudi_catalog',
  'hive.conf.dir' = '/path/to/hive/conf',
  'mode' = 'hms'
);

USE CATALOG hudi_catalog;
CREATE DATABASE db;
USE db;
CREATE TABLE product_daily_price (
  id   BIGINT PRIMARY KEY NOT ENFORCED,
  name STRING,
  price DOUBLE,
  ts   BIGINT,
  dt   STRING
)
PARTITIONED BY (dt)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///tmp/hudi_table',
  'table.type' = 'MERGE_ON_READ',
  'precombine.field' = 'ts',
  'hoodie.cleaner.fileversions.retained' = '20',
  'hoodie.keep.max.commits' = '20',
  'hoodie.datasource.write.hive_style_partitioning' = 'true'
);
INSERT INTO product_daily_price
SELECT 1, 'Lakehouse Book', 50, 1732256367, '2024-11-21';
INSERT INTO product_daily_price + OPTIONS('write.operation' = 'upsert')
SELECT 1, 'Lakehouse Book', 60, 1732256367, '2024-11-21';
UPDATE product_daily_price
SET price = price * 2, ts = 1732258867
WHERE id = 1;
DELETE FROM product_daily_price
WHERE price < 50;
INSERT INTO product_daily_price + OPTIONS('hoodie.keep.max.commits' = '10')
SELECT 2, 'Another Book', 40, 1732256367, '2024-11-21';
