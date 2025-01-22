-- Databricks notebook source
-- Demonstration of running SQL in the notebook
create table test (id int);
insert into test values (1);
select * from test;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Demonstration of using python magic operator in a SQL notebook
-- MAGIC df = spark.sql("select count(*) from test")
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %run "./Module 1. Library Notebook"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # After running the external notebook, we now have access to the myprint() function defined in it
-- MAGIC myprint()

-- COMMAND ----------

-- Clean up the working environment
drop table test;

-- COMMAND ----------


