-- Databricks notebook source
USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

CREATE OR REPLACE TABLE world_population_constraints (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
);

ALTER TABLE world_population_constraints ADD CONSTRAINT rankWithinRange CHECK (rank > 0);

INSERT INTO world_population_constraints VALUES (1, 'India', 'Asia', 1428627663);
INSERT INTO world_population_constraints VALUES (2, 'China', 'Asia', 1425671352);
INSERT INTO world_population_constraints VALUES (3, 'USA', 'North America', 339996563);




-- COMMAND ----------

-- This will fail due to constraint violation
INSERT INTO world_population_constraints VALUES (-1, 'Some country', 'Somewhere', 10000);

-- COMMAND ----------


CREATE OR REPLACE TABLE world_population_constraints (
  rank int,
  country varchar(255),
  continent varchar(255),
  population int
);

INSERT INTO world_population_constraints VALUES (-1, 'Some country', 'Somewhere', 10000);



-- COMMAND ----------

-- This CHECK constraint can not be applied due to existing dirty data
ALTER TABLE world_population_constraints ADD CONSTRAINT rankWithinRange CHECK (rank > 0);


-- COMMAND ----------

-- PRIMARY KEY definition is informational and is not enforced
CREATE OR REPLACE TABLE world_population_constraints (
  rank int primary key,
  country varchar(255),
  continent varchar(255),
  population int
);

INSERT INTO world_population_constraints VALUES (1, 'Some country 1', 'Somewhere', 10000);
INSERT INTO world_population_constraints VALUES (1, 'Some country 2', 'Somewhere', 10000);

SELECT * FROM world_population_constraints;
