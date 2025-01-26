# Databricks notebook source
# Load CSV file into dataframe
from pyspark.sql.types import *

populationSchema = StructType([
  StructField("Rank", IntegerType(), True),
  StructField("Country", StringType(), True),
  StructField("Population", StringType(), True)
])

dfPopulation = spark.read.format("csv") \
  .schema(populationSchema)             \
  .load("abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/WorldPopulation.csv", header=True)

display(dfPopulation)

# COMMAND ----------

from pyspark.sql.functions import col

display(dfPopulation.where(col("Rank")<6))


# COMMAND ----------

display(dfPopulation.filter(col("Rank")<6))

# COMMAND ----------

display(dfPopulation.filter((col("Rank")<6) & (col("Population")<300000000)))

# COMMAND ----------

# This logic will fail because the lowest logic expressions are not enclosed in parenthesis
display(dfPopulation.filter((col("Rank")<6) & col("Population")<300000000))

# COMMAND ----------

display(dfPopulation.filter((col("Rank")<6) | (col("Population")<100000000)))

# COMMAND ----------

display(dfPopulation.filter(~(col("Rank")<45)))