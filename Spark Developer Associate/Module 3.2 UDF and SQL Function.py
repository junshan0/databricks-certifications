# Databricks notebook source
# Load CSV file into dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import col

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

# Define a UDF to add 1 to the age column
def convertToMillion(population):
    return int(int(population)/1000000)

# COMMAND ----------

convertToMillionUDF = udf(convertToMillion, IntegerType())
dfPopulation.withColumn("result", convertToMillionUDF(col("Population"))).display()


# COMMAND ----------

for row in dfPopulation.collect(): print(row)

# COMMAND ----------

# MAGIC %scala
# MAGIC val values = List(List("1", "Sales") ,List("2", "Cost")).map(x =>(x(0), x(1)))
# MAGIC import spark.implicits._
# MAGIC val df = values.toDF
# MAGIC
# MAGIC df.collect().foreach(row => println("RESULT:", row))

# COMMAND ----------

dfPopulation.createOrReplaceTempView("world_population")

# COMMAND ----------

spark.udf.register("CONVERT_TO_MILLIONS", convertToMillion)
spark.sql("SELECT *, CONVERT_TO_MILLIONS(Population) AS result FROM world_population").display()
