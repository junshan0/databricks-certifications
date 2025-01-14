
CREATE CATALOG data_analyst_demo MANAGED LOCATION 'abfss://<container>@<storage-account>.dfs.core.windows.net/<folder-path>';
USE CATALOG data_analyst_demo;
USE SCHEMA default;

create table managed_population (
  country_name varchar(100),
  population int
);

INSERT INTO managed_population VALUES ('India', 1428627663);
INSERT INTO managed_population VALUES ('China', 1425671352);
-- Demonstrate the folder and number of files after INSERT

CREATE TABLE external_population (
  country_name varchar(100),
  population int
)
LOCATION 'abfss://<container>@<storage account>.dfs.core.windows.net/newTableFolder/';

INSERT INTO external_population VALUES ('India', 1428627663);
INSERT INTO external_population VALUES ('China', 1425671352);
-- Demonstrate the folder and number of files after INSERT

ALTER TABLE external_population RENAME TO external_population_azure;

DROP TABLE external_population_azure; 
-- Check data file existence
DROP TABLE managed_population; 