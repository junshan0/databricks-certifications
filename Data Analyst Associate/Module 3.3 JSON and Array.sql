-- Create a table for JSON processing.
-- This table contains one column, which stores the JSON document, one document in each row 
CREATE OR REPLACE TABLE world_population_json AS
select '{"Country":"India","Population":"1428627663"}' as raw_json;

-- Accessing JSON attribute by using the : operator
select raw_json:Country
from world_population_json;

CREATE OR REPLACE TABLE world_population_json AS
select '{"Country":"India","Population":"1428627663","Land Area":{"SqKm":"2973190"},"Large Cities":["New Delhi", "Mumbai"]}' as raw_json;

select
  raw_json:`Land Area`.SqKm,
  raw_json:`Large Cities`[0]
from world_population_json;

-- Array operation: create an array from a list of values
SELECT array(1,2,3,4);
-- Array operation: create an array from a list of smaller arrays
SELECT array(array(1, 2), array(3, 4));
SELECT flatten(array(array(1, 2), array(3, 4)));
-- Array operation: flattern array into a list
SELECT explode(array(1,2,3,4))
