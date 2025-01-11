-- Databricks notebook source
-- Switch to the working catalog/schema
use catalog DataAtExternalLocation;
use schema default;


-- COMMAND ----------

-- Create base table, with COMMENT and generated column
CREATE TABLE events(
    eventId BIGINT,
    eventName STRING,
    eventType STRING,
    eventTime TIMESTAMP,
    eventDate date GENERATED ALWAYS AS (CAST(eventTime AS DATE))
) 
COMMENT 'Add your table comments here';


-- COMMAND ----------

-- INSERT with INTO clause and value list
INSERT INTO events (eventId, eventName, eventType, eventTime)
VALUES (1, 'Tom birthday', 'Party', '2024-09-01 12:00:00');

-- COMMAND ----------

select * from events;

-- COMMAND ----------

-- Demonstrate the behavior of LIKE and CLONE
-- See Powerpoint slides for the table folder after each operation
CREATE TABLE Events_Dup_LIKE LIKE Events;
CREATE TABLE Events_Dup_DEEP DEEP CLONE Events;
CREATE TABLE Events_Dup_SHALLOW SHALLOW CLONE Events;
CREATE TABLE Events_Dup_CLONE CLONE Events;
-- Default behavior of CLONE is DEEP CLONE

-- COMMAND ----------

-- CREATE TABLE ... AS SELECT query
CREATE TABLE parties 
AS 
SELECT *
FROM events
WHERE eventType = 'Party';

-- COMMAND ----------

-- INSERT INTO will keep the existing data 
INSERT INTO events (eventId, eventName, eventType, eventTime)
VALUES (2, 'Allen soccer', 'Soccer', '2024-09-11 12:00:00');

SELECT * FROM Events;


-- COMMAND ----------

-- INSERT OVERWRITE will remove all existing data
-- Compare this result against the result in previous cell which used INSERT INTO
INSERT OVERWRITE events (eventId, eventName, eventType, eventTime)
VALUES (3, 'Jim baseball', 'Baseball', '2024-09-21 12:00:00');

SELECT * FROM Events;

-- COMMAND ----------

-- INSERT INTO ... REPLACE will replace the existing data that matches the WHERE condition
-- Other records that do not meet the WHERE condition will be untouched
INSERT INTO events
REPLACE WHERE eventId = 3
VALUES (3, 'Mary piano', 'Piano', '2024-09-30 12:00:00', '2024-09-30');

SELECT * FROM Events;

-- COMMAND ----------

-- History of the table, including each transaction's operator, operation, and version number and timestamp
DESCRIBE HISTORY events;

-- COMMAND ----------

-- Track table historical state using version number
SELECT * FROM events@v1

-- COMMAND ----------

-- Tracking table history state using timestamp
SELECT * FROM events@20240807201200000;