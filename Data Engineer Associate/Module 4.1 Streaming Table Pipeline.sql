-- Databricks notebook source
-- Create the DLT using streaming table from Databricks Autoloader 
-- By specifying the source is cloud_files(), you are using Autoloader as the source
-- The Autoloader will monitor the Azure blob storage location and pull in new files as they are saved into this location
CREATE OR REFRESH STREAMING TABLE world_population_streaming_bronze
AS 
SELECT 
  Rank,
  Country,
  `2023 Population` as CountryPopulation
FROM cloud_files('abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>');

-- COMMAND ----------

-- Create another layer of processing in the DLT using streaming table
--   simulating the bronze to silver layer conversion in medallion architecture 
-- Note that the target is a streaming table, so the source from which the data comes from must be streaming too
-- Running without the stream() conversion will make the pipeline fail
CREATE OR REFRESH STREAMING TABLE world_population_streaming_silver
AS SELECT
  Rank,
  Country,
  CountryPopulation
FROM stream(live.world_population_streaming_bronze);
-- FROM live.world_population_streaming_bronze;