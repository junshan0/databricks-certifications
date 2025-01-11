-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Read data from a JSON file in the Databricks workspace
-- MAGIC # In Spark, .option("multiline","true") must be specified to properly parse the JSON schema
-- MAGIC spark.read.format("json").option("multiline","true").load("file:/Workspace/Users/databricks_certifications@outlook.com/file.json").show()

-- COMMAND ----------

-- Direct reading from an external CSV file. 
-- Other file formats such as JSON are also supported
select * from csv.`abfss://<container>@<storage account>.dfs.core.windows.net/PopulationFolder/`

-- COMMAND ----------

-- Switch to the right catalog
-- Prepare for Unity Catalog object management code 
use catalog dataengineerassociate;
use schema default;

-- COMMAND ----------

-- Create a managed table
CREATE TABLE managed_table_student (
  student_id int,
  student_name varchar(100)
);

-- Create an external table in an external location
CREATE TABLE external_table_delta
LOCATION 'abfss://<container>@<storage account>.dfs.core.windows.net/externaltable1/';

-- Create an external volume in an external location
CREATE EXTERNAL VOLUME external_volume
LOCATION 'abfss://<container>@<storage account>.dfs.core.windows.net/externalvolume/'

-- COMMAND ----------

-- Switch to the catalog that was created in an external location
use catalog DataAtExternalLocation;
use schema default;

-- Create a managed table
CREATE TABLE managed_table_student (
  student_id int,
  student_name varchar(100)
);

-- Create an external table in an external location
CREATE TABLE external_table_delta
LOCATION 'abfss://<container>@<storage account>.dfs.core.windows.net/externaltable2/';



-- COMMAND ----------


-- External tables and external volumes must be defined in  external locations that have been defined within the workspace
-- Otherwise, when you try to use an external location that was not define for external tables or external volumes,
--   an error will occur because Databricks does not have the proper credential to access that location. 
CREATE TABLE invalid_external_table
LOCATION 'abfss://<invalid container>@<storage account>.dfs.core.windows.net/externaltable2/';
