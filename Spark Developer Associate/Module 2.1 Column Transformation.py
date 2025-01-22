# Databricks notebook source
from pyspark.sql.types import *

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

from pyspark.sql.functions import col
dfPopulation.withColumnRenamed("CID", "Rank").show()

# COMMAND ----------

# This will result in an error since we should use string column name in withColumnRenamed instead of column object
dfPopulation.withColumnRenamed("FirstName", col("Country")).show()

# COMMAND ----------

dfPopulation.show()

# COMMAND ----------

display(dfPopulation.withColumn("Population", col("LastName")))

# COMMAND ----------

display(dfPopulation.withColumn("Population", col("LastName").cast(IntegerType())))

# COMMAND ----------

from pyspark.sql.functions import lit
dfPopulation.withColumn("planet", lit("Earth")).show()

# COMMAND ----------

l = [
    (1, "Tom", "Cat", '2000-01-01'),
    (2, "Jerry", "Mouse", '2001-01-01'),
    (3, "Mario", None, '2002-01-01'),
    (4, "Luigi", None, '2003-01-01'),
    (5, "Princess Peach", None, '2004-01-01')
]

customerSchema = StructType([
    StructField("ID",IntegerType(),True),
    StructField("FirstName",StringType(),True),
    StructField("LastName",StringType(),True),
    StructField("Birthday",StringType(),True)
])

dfCustomer = spark.createDataFrame(l, schema = customerSchema)

dfCustomer.show()

# COMMAND ----------

display(dfCustomer.withColumn("WordsInName", (split(col("FirstName"), " "))))

# COMMAND ----------

from pyspark.sql.functions import explode, split

display(dfCustomer.withColumn("WordsInName", explode(split(col("FirstName"), " "))))

# COMMAND ----------

display(dfCustomer.na.fill("Unknown", "LastName"))

# COMMAND ----------

# This will fail because it is using column insead of string column name
display(dfCustomer.na.fill("Unknown", col("LastName")))

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, cast
display(dfCustomer.withColumn("Birth Date", col("Birthday").cast("timestamp")))