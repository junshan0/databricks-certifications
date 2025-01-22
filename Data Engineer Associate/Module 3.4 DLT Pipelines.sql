-- Databricks notebook source
-- Switch to the working catalog/schema 
use catalog DataAtExternalLocation;
use schema default;

-- COMMAND ----------

-- After the initial creation of the DLT materialized view, there are 10 rows in the table
select count(*) from world_population_raw;

-- COMMAND ----------

-- After the initial creation of the streaming table, the table has 10 rows from the first file
-- As new files added to the folder, their data flows into the streaming table and the row count keeps on increasing. 
select count(*) from world_population_streaming_raw;
-- Upload almost immediately