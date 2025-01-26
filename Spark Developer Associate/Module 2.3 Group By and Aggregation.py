# Databricks notebook source
# Load CSV file into dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import col

f500Schema = StructType([
  StructField("name", StringType(), True),
  StructField("rank", StringType(), True),
  StructField("year", StringType(), True),
  StructField("industry", StringType(), True),
  StructField("sector", StringType(), True),
  StructField("headquarters_state", StringType(), True),
  StructField("headquarters_city", StringType(), True),
  StructField("market_value_mil", IntegerType(), True),
  StructField("revenue_mil", IntegerType(), True),
  StructField("profit_mil", IntegerType(), True),
  StructField("asset_mil", IntegerType(), True),
  StructField("employees", IntegerType(), True),
  StructField("founder_is_ceo", StringType(), True),
  StructField("female_ceo", StringType(), True),
  StructField("newcomer_to_fortune_500", StringType(), True),
  StructField("global_500", StringType(), True),
])

dfF500 = spark.read     \
  .schema(f500Schema)   \
  .csv("abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/Fortune500Companies.csv", header=True)

display(dfF500)

# COMMAND ----------

dfF500.printSchema()

# COMMAND ----------

(dfF500.groupBy("industry"))

# COMMAND ----------

(dfF500.groupBy("industry", "sector"))

# COMMAND ----------

# This code will fail because the default sum() function referenced is not the sum() that we should use
dfF500.groupBy("industry", "sector").agg(sum("market_value_mil"))

# COMMAND ----------

from pyspark.sql.functions import sum
dfF500.groupBy("industry", "sector").agg(sum("market_value_mil")).display()

# COMMAND ----------

from pyspark.sql.functions import mean
dfF500.groupBy("industry", "sector").agg(mean("employees").alias("average_headcount")).display()

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct
dfF500.groupBy("sector").agg(approx_count_distinct("name").alias("average_headcount")).display()