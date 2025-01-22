# Databricks notebook source
# Load list into dataframe
from pyspark.sql.types import *

l = [
    {'MID': 1, 'MName': 'Sales'},
    {'MID': 2, 'MName': 'Cost'},
]

dfMetricDefinition = spark.createDataFrame(l)

dfMetricDefinition.show()

# COMMAND ----------

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

dfMetricDefinition.printSchema()

# COMMAND ----------

dfMetricDefinition.describe()

# COMMAND ----------

dfMetricDefinition.describe("MID")

# COMMAND ----------

dfMetricDefinition.select()

# COMMAND ----------

dfMetricDefinition.select().show()

# COMMAND ----------

dfMetricDefinition.select("MName").show()

# COMMAND ----------

dfMetricDefinition.collect()

# COMMAND ----------

dfMetricDefinition.take(1)

# COMMAND ----------

dfMetricDefinition.head(1)

# COMMAND ----------

[print(row) for row in dfMetricDefinition.collect()]

# COMMAND ----------

dfMetricDefinition.first

# COMMAND ----------

# MAGIC %scala
# MAGIC val values = List(List("1", "Sales") ,List("2", "Cost")).map(x =>(x(0), x(1)))
# MAGIC import spark.implicits._
# MAGIC val df = values.toDF
# MAGIC df.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val values = List(List("1", "Sales") ,List("2", "Cost")).map(x =>(x(0), x(1)))
# MAGIC import spark.implicits._
# MAGIC val df = values.toDF
# MAGIC df.first

# COMMAND ----------

# MAGIC %scala
# MAGIC val values = List(List("1", "Sales") ,List("2", "Cost")).map(x =>(x(0), x(1)))
# MAGIC import spark.implicits._
# MAGIC val df = values.toDF
# MAGIC df.first.getAs[String]("_2")

# COMMAND ----------

dfMetricDefinition.orderBy("MName").show()

# COMMAND ----------

dfMetricDefinition.orderBy("MName", ascending=True).show()

# COMMAND ----------

dfMetricDefinition.orderBy("MName", ascending=False).show()

# COMMAND ----------

from pyspark.sql.functions import col
dfMetricDefinition.orderBy(col("MName"), ascending=False).show()

# COMMAND ----------

dfMetricDefinition.sort("MName").show()

# COMMAND ----------

dfMetricDefinition.sort("MName", ascending=False).show()
