-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC SQL code for Module 2.4: JSON, Array, and Function

-- COMMAND ----------

-- Create a table for JSON processing.
-- This table contains one column, which stores the JSON document, one document in each row 
CREATE TABLE world_population AS
select '{"Country":"India","Population":"1428627663"}' as raw_json;

-- Accessing JSON attribute by using the : operator
select raw_json:Country
from world_population;


-- COMMAND ----------

-- Accessing attributes with nested value and array
DROP TABLE world_population;

CREATE TABLE world_population AS
select '{"Country":"India","Population":"1428627663","Land Area":{"SqKm":"2973190"},"Large Cities":["New Delhi", "Mumbai"]}' as raw_json;

select
  raw_json:`Land Area`.SqKm,
  raw_json:`Large Cities`[0]
from world_population

-- COMMAND ----------

-- The STRUCT data type in Databricks
SELECT named_struct('Company', 'Databricks', 'Year', 2024)

-- COMMAND ----------

-- Convert a JSON document into STRUCT
SELECT from_json('{"Country":"India","Population":"1428627663"}', 'Country STRING, Population STRING')

-- COMMAND ----------

-- Array operation: create an array from a list of values
SELECT array(1,2,3,4);

-- COMMAND ----------

-- Array operation: create an array from a list of smaller arrays
SELECT flatten(array(array(1, 2), array(3, 4)));

-- COMMAND ----------

-- Array operation: flattern array into a list
SELECT explode(array(1,2,3,4))

-- COMMAND ----------

-- Create a SQL function
CREATE FUNCTION convert_f_to_c(unit STRING, temp DOUBLE)
RETURNS DOUBLE
RETURN 
  CASE
    WHEN unit = "F" THEN (temp - 32) * (5/9)
    ELSE temp
  END;

-- COMMAND ----------

-- Check the celsium degree corresponding to 32 degree in Farenheit
SELECT convert_f_to_c('F', 32);

-- COMMAND ----------

-- Use DESCRIBE FUNCTION to get the location, type, input parameters, and return type of the function
DESCRIBE FUNCTION convert_f_to_c;