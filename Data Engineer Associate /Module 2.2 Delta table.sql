-- Databricks notebook source
-- Create a delta table by not specifying the USING clause

use catalog DataAtExternalLocation;

use schema default;

CREATE TABLE managed_table_student (
  student_id int,
  student_name varchar(100)
);

-- After the execution, the table folder will be created, with _delta_log subfolder but no data file


-- COMMAND ----------

-- Inserting data into the table will cause data files (parquet) created in the table folder
insert into managed_table_student values (1, 'Tom');
insert into managed_table_student values (2, 'Mary');
insert into managed_table_student values (3, 'Josh');
insert into managed_table_student values (4, 'Julia');
insert into managed_table_student values (5, 'Fred');
insert into managed_table_student values (6, 'Sarah');


-- COMMAND ----------

-- By specifying the USING clause, a CSV table is created
-- Databricks only manages delta table. Tables of other format (such as CSV) must be defined as external tables.
-- As such the LOCATION clause must be specified too   
CREATE TABLE csv_table (
  country_name varchar(100),
  population int
)
USING CSV
LOCATION 'abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/csvTableFolder/'
-- After execution, the CSV table is created in the catalog but its folder is not created yet.


-- COMMAND ----------

-- Once data is inserted into the CSV table, a table folder is created and a CSV file is created in the table folder.
insert into csv_table values ('India', 1428627663);

-- COMMAND ----------

use catalog DataAtExternalLocation;

use schema default;

-- COMMAND ----------

-- The DESCRIBE statement will return the schema of the table  
describe managed_table_student;

-- COMMAND ----------

-- The DESCRIBE EXTENDED statement will return all properties of the table
describe extended managed_table_student
