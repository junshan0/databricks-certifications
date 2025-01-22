# Databricks notebook source
# Load list into dataframe
from pyspark.sql.types import *

l = [
    {'MID': 1, 'MName': 'Sales'},
    {'MID': 2, 'MName': 'Cost'},
]

metricSchema = StructType([
    StructField("MID",IntegerType(),True),
    StructField("MName",StringType(),True)
])

dfMetricDefinition = spark.createDataFrame(l, schema = metricSchema)

dfMetricDefinition.show()

# COMMAND ----------

# Load JSON file into dataframe
countrySchema = StructType([
  StructField("Country", StringType(), True),
  StructField("Population", StringType(), True)
])

dfCountry = spark.read.format("json") \
  .option("multiline","true")         \
  .schema(countrySchema)              \
  .load("abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/file.json")

display(dfCountry)

# COMMAND ----------

# Load JSON file into dataframe
customerSchema = StructType([
  StructField("CID", IntegerType(), True),
  StructField("FirstName", StringType(), True),
  StructField("LastName", StringType(), True)
])

dfCustomer = spark.read.format("json") \
  .schema(customerSchema)              \
  .load("abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/customer.json")

display(dfCustomer)

# COMMAND ----------

# Load CSV into dataframe
dfPopulation = spark.read.format("csv")         \
  .option("delimiter",",")                      \
  .option("header", "true")                     \
  .load("abfss://dbkexternallocation@dbkexternallocation.dfs.core.windows.net/WorldPopulation.csv")

display(dfPopulation)

# COMMAND ----------

# Apply filter

# COMMAND ----------

# Apply join

# COMMAND ----------

# Write to parquet
