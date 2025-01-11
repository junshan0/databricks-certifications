-- Databricks notebook source
-- Create the DLT using materialized view from the Azure blob storage location 
CREATE OR REFRESH MATERIALIZED VIEW world_population_raw
AS 
SELECT 
  Rank,
  Country,
  `2023 Population` as CountryPopulation
FROM read_files(
  'abfss://<container>@<storage account>.dfs.core.windows.net/PopulationFolder/',
  format => 'csv',
  header => true,
  mode => 'FAILFAST')

-- COMMAND ----------

-- Create a second processing layer in the DLT using materialized view, 
--   simulating the bronze to silve layer conversion in a typical modallion architecture
CREATE OR REFRESH MATERIALIZED VIEW world_population_prepared 
AS SELECT
  Rank,
  Country,
  CountryPopulation
FROM live.world_population_raw;