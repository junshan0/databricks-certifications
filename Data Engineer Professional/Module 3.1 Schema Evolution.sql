-- Databricks notebook source
USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

CREATE OR REPLACE TABLE world_population_source (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int,
  land int
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

INSERT INTO world_population_source VALUES (4, 'Indonesia', 'Asia', 277534122, 1811570);


-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO world_population_target
USING world_population_source
ON world_population_source.rank = world_population_target.rank
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;

SELECT * FROM world_population_target;
