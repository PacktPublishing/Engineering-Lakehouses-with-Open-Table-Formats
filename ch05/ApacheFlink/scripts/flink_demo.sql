CREATE CATALOG delta_catalog WITH (
  'type' = 'delta-catalog',
  'catalog-type' = 'in-memory'
);

USE CATALOG delta_catalog;

CREATE DATABASE IF NOT EXISTS customer_data;

USE customer_data;

CREATE TABLE customers (
  customer_id BIGINT,
  first_name  STRING,
  last_name   STRING,
  email       STRING,
  charges     FLOAT,
  state       STRING
)
PARTITIONED BY (state)
WITH (
  'connector' = 'delta',
  'table-path' = 'file:///warehouse/customers'
);

INSERT INTO customers VALUES
  (1, 'John', 'Doe', 'john@example.com', 100.0, 'CA');

SET execution.runtime-mode = 'batch';

INSERT OVERWRITE customers VALUES
  (2, 'Jane', 'Smith', 'jane@example.com', 200.0, 'NY');

SELECT * FROM customers;
