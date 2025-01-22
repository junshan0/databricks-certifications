-- Databricks notebook source

USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

DROP TABLE world_population_cdc;
CREATE OR REPLACE TABLE world_population_cdc (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
) TBLPROPERTIES (delta.enableChangeDataFeed = true);

INSERT INTO world_population_cdc VALUES (1, 'India', 'Asia', 1428627663);
INSERT INTO world_population_cdc VALUES (2, 'China', 'Asia', 1425671352);
INSERT INTO world_population_cdc VALUES (3, 'USA', 'North America', 339996563);





-- COMMAND ----------

describe history world_population_partition

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.table("world_population_cdc").display()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.readStream.option("readChangeFeed", "true").table("world_population_cdc").display()
-- MAGIC

-- COMMAND ----------

-- Note the last parameter is the starting commit version
SELECT * FROM table_changes('world_population_cdc', 0)

-- COMMAND ----------

-- Note the last parameter is the starting commit version
SELECT * FROM table_changes('world_population_cdc', 2)

-- COMMAND ----------

UPDATE world_population_cdc 
SET Country = 'United States of America'
WHERE rank = 3;

INSERT INTO world_population_cdc VALUES (4, 'Indonesia', 'Asia', 277534122);

DELETE FROM world_population_cdc WHERE rank = 4;

SELECT * FROM table_changes('world_population_cdc', 0)
