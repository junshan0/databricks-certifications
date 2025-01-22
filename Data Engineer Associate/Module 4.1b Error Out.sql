-- Databricks notebook source
-- This notebook contains an intentional mistake to demonstrate error handling
SELECT err FROM dataatexternallocation.default.world_population; 

-- COMMAND ----------

CREATE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN area(x, x);