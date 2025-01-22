-- Databricks notebook source
DROP TABLE dataatexternallocation.default.world_population_partition;

USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

CREATE OR REPLACE TABLE world_population_partition (
  rank int,
  country varchar(255),
  continent varchar(255)
) PARTITIONED BY (continent);




-- COMMAND ----------

-- Adding data to the table
-- After these statements are executed, you will see the subfolders for each partition
insert into world_population_partition values(1, 'India', 'Asia');
insert into world_population_partition values(2, 'China', 'Asia');
insert into world_population_partition values(3, 'USA', 'North America');


-- COMMAND ----------

 
USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

DESCRIBE world_population_partition;

-- COMMAND ----------

DESCRIBE EXTENDED world_population_partition;

-- COMMAND ----------

SHOW TBLPROPERTIES world_population_partition;
