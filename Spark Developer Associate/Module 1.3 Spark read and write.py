# Databricks notebook source
from pyspark.sql.types import *

# Load JSON file into dataframe
countrySchema = StructType([
  StructField("Country", StringType(), True),
  StructField("Population", StringType(), True)
])

dfCountry = spark.read.format("json") \
  .option("multiline","true")         \
  .schema(countrySchema)              \
  .load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>")

display(dfCountry)


# COMMAND ----------


# Load JSON file into dataframe
countrySchema = StructType([
  StructField("Country", StringType(), True),
  StructField("Population", StringType(), True)
])

dfCountry = spark.read                \
  .option("multiline","true")         \
  .schema(countrySchema)              \
  .json("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>")

display(dfCountry)


# COMMAND ----------

# Load CSV file into dataframe
populationSchema = StructType([
  StructField("CID", IntegerType(), True),
  StructField("FirstName", StringType(), True),
  StructField("LastName", StringType(), True)
])

dfPopulation = spark.read.format("csv") \
  .schema(populationSchema)             \
  .load("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>")

display(dfPopulation)

# COMMAND ----------

# Load CSV file into dataframe
populationSchema = StructType([
  StructField("CID", IntegerType(), True),
  StructField("FirstName", StringType(), True),
  StructField("LastName", StringType(), True)
])

dfPopulation = spark.read      \
  .schema(populationSchema)    \
  .csv("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>", header=True)

display(dfPopulation)

# COMMAND ----------

dfCustomer.write         \
    .mode("overwrite")   \
    .saveAsTable("sparkCustomer") # Delta table


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE sparkCustomer;

# COMMAND ----------

dfPopulation.write    \
    .mode("append")  \
    .csv("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-file>")
