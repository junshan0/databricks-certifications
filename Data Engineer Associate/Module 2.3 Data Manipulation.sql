-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC SQL code for Module 2.3: Data transformations

-- COMMAND ----------

use catalog DataAtExternalLocation;

use schema default;

-- COMMAND ----------

-- Raw data in the table has two student names starting with J
SELECT * FROM managed_table_student

-- COMMAND ----------

-- Count how many rows in the table has two student names starting with J
SELECT COUNT_IF(student_name LIKE 'J%') FROM managed_table_student

-- COMMAND ----------

-- Casting string into timestamp
SELECT cast('2024-10-18' AS timestamp)

-- COMMAND ----------

-- Casting timestamp into string
SELECT cast(current_timestamp() AS varchar(100))


-- COMMAND ----------

-- Extract the YEAR field from timestamp
SELECT extract(YEAR FROM current_timestamp())

-- COMMAND ----------

-- The standard way of converting a date into string
select cast(current_date() as varchar(20))

-- COMMAND ----------

-- The simple way of converting a date into string
select string(current_date())

-- COMMAND ----------

-- Example of CASE WHEN ELSE END statement
SELECT 
  student_name,
  CASE WHEN student_name LIKE 'J%' THEN 'J' 
  WHEN student_name LIKE 'T%' THEN 'T' 
  ELSE 'Neither J nor T' END 
FROM managed_table_student

-- COMMAND ----------

-- Using PIVOT to move a series of rows onto columns
SELECT No1, No2, No3, No4
FROM
  (SELECT rank, `2023 population` FROM world_population)
  PIVOT (
    sum(`2023 population`) FOR rank IN (1 AS No1, 2 AS No2, 3 AS No3, 4 AS No4)
  )
;
-- The indented rows of this SQL statement (the part after FROM) provides a relational set that was formed by 
--   using a data set (the SELECT ... FROM world_population subqery) followed by a PIVOT clause.
-- The PIVOT clause indicates which row(s) from the subquery should be moved into column, and the column name
-- Here, row(s) with rank = 1 will be pivoted into the No1 column, 2 into No2, etc.