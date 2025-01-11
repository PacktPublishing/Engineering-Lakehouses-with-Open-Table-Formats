-- Create an Iceberg catalog
CREATE CATALOG dynamo_catalog WITH ( 

  'type' = 'iceberg', 

  'catalog-impl' = 'org.apache.iceberg.aws.dynamodb.DynamoDbCatalog', 

  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', 

  'client.assume-role.region' = 'us-east-1', 

  'warehouse' = 's3://warehouse', 

  's3.endpoint' = 'http://storage:9000', 

  's3.path-style-access' = 'true', 

  'dynamodb.table-name' = 'iceberg-catalog', 

  'dynamodb.endpoint' = 'http://dynamodb-local:8000' 

); 


-- List the catalogs
show CATALOGS;

-- Use the Iceberg catalog for further operations
USE CATALOG dynamo_catalog; 

-- Create a new database
CREATE DATABASE db; 

USE db; 

-- List out all the databases
show databases;

-- Create an Iceberg Table
CREATE TABLE event_logs ( 

  event_id STRING, 

  event_type STRING, 

  event_source STRING, 

  event_time TIMESTAMP(3), 

  event_details STRING 

) PARTITIONED BY (event_time); 

-- List out all the Iceberg tables
SHOW TABLES;


-- Insert records to the Iceberg table
INSERT INTO event_logs VALUES ('event1', 'click', 'app', TIMESTAMP '2023-10-26 10:15:00', 'User clicked button'); 

-- Insert Overwrite
SET execution.runtime-mode = batch; 

INSERT OVERWRITE `event_logs` VALUES('event1', 'click', 'app', TIMESTAMP '2023-10-26 10:15:00', 'User clicked button'); 

-- Upsert
-- Method 1:
CREATE TABLE user_activities ( 

      `activity_id` STRING UNIQUE, 

      `user_id` STRING NOT NULL, 

      `activity_type` STRING, 

      `activity_time` TIMESTAMP(3), 

      `activity_details` STRING, 

      PRIMARY KEY(`activity_id`) NOT ENFORCED 

) WITH ('format-version'='2', 'write.upsert.enabled'='true'); 

INSERT INTO user_activities VALUES ('activity123', 'user26', 'login', TIMESTAMP '2023-10-26 10:15:00', 'User logged in from laptop');

-- Method 2:
INSERT INTO user_activities /*+ OPTIONS('upsert-enabled'='true') */  

VALUES ('activity123', 'user42', 'login', TIMESTAMP '2023-10-26 11:00:00', 'User logged in from mobile'); 

-- Read queries
-- Batch:
SET execution.runtime-mode = batch; 

SELECT * FROM events_log; 

-- Streams:
SET execution.runtime-mode = streaming; 

SET table.dynamic-table-options.enabled=true; 

SELECT * FROM user_activities /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s') */; 
