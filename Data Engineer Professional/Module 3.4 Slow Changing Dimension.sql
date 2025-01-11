-- Databricks notebook source
USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

CREATE OR REPLACE TABLE world_population_source (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
);

CREATE OR REPLACE TABLE world_population_target (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
);

INSERT INTO world_population_target VALUES (1, 'India', 'Asia', 1428627663);
INSERT INTO world_population_target VALUES (2, 'China', 'Asia', 1425671352);
INSERT INTO world_population_target VALUES (3, 'USA', 'North America', 339996563);

INSERT INTO world_population_source VALUES (3, 'United States', 'North America', 339996563);


-- COMMAND ----------


--Type 1 SCD: Overwrite
MERGE WITH SCHEMA EVOLUTION INTO world_population_target
USING world_population_source
ON world_population_source.rank = world_population_target.rank
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

SELECT * FROM world_population_target;


-- COMMAND ----------

USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

CREATE OR REPLACE TABLE world_population_source (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
);

CREATE OR REPLACE TABLE world_population_target (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int,
  start_date date,
  end_date date
);

INSERT INTO world_population_target VALUES (1, 'India', 'Asia', 1428627663, '2024-01-01', '2999-12-31');
INSERT INTO world_population_target VALUES (2, 'China', 'Asia', 1425671352, '2024-01-01', '2999-12-31');
INSERT INTO world_population_target VALUES (3, 'USA', 'North America', 339996563, '2024-01-01', '2999-12-31');

INSERT INTO world_population_source VALUES (3, 'United States', 'North America', 339996563);


-- COMMAND ----------

--Type 2 SCD: Add new
MERGE WITH SCHEMA EVOLUTION INTO world_population_target
USING (
  SELECT *, current_date() as start_date, '2999-12-31' as end_date
  FROM world_population_source
  UNION ALL
  SELECT 
    world_population_source.*, 
    world_population_target.start_date, 
    world_population_target.end_date
  FROM world_population_source
  JOIN world_population_target
    on world_population_target.rank = world_population_source.rank
) as source_table
ON source_table.rank = world_population_target.rank
AND source_table.start_date = world_population_target.start_date
WHEN MATCHED and world_population_target.end_date = '2999-12-31' THEN
  UPDATE SET world_population_target.end_date = current_date()
WHEN NOT MATCHED THEN
  INSERT (rank, country, continent, population, start_date, end_date) 
  VALUES (source_table.rank, source_table.country, source_table.continent, source_table.population, source_table.start_date, source_table.end_date);

SELECT * FROM world_population_target order by rank, end_date;
