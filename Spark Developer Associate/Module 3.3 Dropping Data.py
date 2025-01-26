# Databricks notebook source

from pyspark.sql.types import *

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

dfCustomer.display()

# COMMAND ----------

dfCustomer.drop("Birthday").display()

# COMMAND ----------

dfCustomer.na.drop(how="any").display()

# COMMAND ----------

dfCustomer.na.drop("any").display()


# COMMAND ----------

dfCustomer.na.drop(how="all").display()

# COMMAND ----------

l = [
    (1, "Tom", "Cat", '2000-01-01'),
    (2, "Jerry", "Mouse", '2001-01-01'),
    (3, "Mario", None, '2002-01-01'),
    (4, "Luigi", None, '2003-01-01'),
    (4, "Luigi", None, '2003-02-01'),
    (5, "Princess Peach", None, '2004-01-01'),
    (5, "Princess Peach", None, '2004-01-01')
]

customerSchema = StructType([
    StructField("ID",IntegerType(),True),
    StructField("FirstName",StringType(),True),
    StructField("LastName",StringType(),True),
    StructField("Birthday",StringType(),True)
])

dfCustomer = spark.createDataFrame(l, schema = customerSchema)

dfCustomer.display()

# COMMAND ----------

dfCustomer.dropDuplicates().display()

# COMMAND ----------

dfCustomer.dropDuplicates(["FirstName", "LastName"]).display()

# COMMAND ----------

dfCustomer.select(["FirstName", "LastName"]).distinct().display()