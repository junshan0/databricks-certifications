-- Databricks notebook source
USE CATALOG dataatexternallocation;
USE SCHEMA DEFAULT;

-- The Region parameter is not required. It is added here just to show the ability to use a parameter
-- However if you have a parameter defined, it must be used when assigned to the table
CREATE OR REPLACE FUNCTION finance_team_filter(region STRING)
RETURN IS_ACCOUNT_GROUP_MEMBER('finance');

CREATE OR REPLACE TABLE sales (region STRING, sales INT);




-- COMMAND ----------

INSERT INTO sales VALUES ('North', 1000);
INSERT INTO sales VALUES ('South', 2000);
SELECT * FROM sales;


-- COMMAND ----------

ALTER TABLE sales SET ROW FILTER finance_team_filter ON (region);
-- We need to designate the ON clause because the function has a parameter
SELECT * FROM sales;


-- COMMAND ----------

CREATE OR REPLACE TABLE sales_mask (region STRING, sales INT);
INSERT INTO sales_mask VALUES ('North', 1000);
INSERT INTO sales_mask VALUES ('South', 2000);
SELECT * FROM sales_mask;



-- COMMAND ----------

CREATE FUNCTION sales_mask(sales INT)
  RETURN 
    CASE WHEN is_member('finance') THEN sales ELSE '$$$' END;
ALTER TABLE sales_mask ALTER COLUMN sales SET MASK sales_mask;
SELECT * FROM sales_mask;

-- is_account_group_member() : Returns TRUE if the current user is a member of a specific account-level group. 
-- is_member() : Returns TRUE if the current user is a member of a specific workspace-level group.