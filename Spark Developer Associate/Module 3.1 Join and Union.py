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

# Load list into dataframe

l = [
    {'Rank': 1, 'Description': 'Top'},
    {'Rank': 2, 'Description': 'Runner up'},
    {'Rank': 3, 'Description': 'Follower'},
]

dfTitle = spark.createDataFrame(l)

dfF500.join(dfTitle, "Rank").display()



# COMMAND ----------

dfF500.join(dfTitle, "Rank", "left").display()


# COMMAND ----------

# Load list into dataframe

l = [
    {'Rank': 1, 'Year': 1996, 'Description': 'Top 1996'},
    {'Rank': 2, 'Year': 1996, 'Description': 'Runner up 1996'},
    {'Rank': 3, 'Year': 1996, 'Description': 'Follower 1996'},
]

dfTitle = spark.createDataFrame(l)

dfF500.join(dfTitle, ['rank', 'year']).display()
# Not case sensitive for column names

# COMMAND ----------


dfF500.join(dfTitle, (dfTitle.Rank == dfF500.rank) & (dfTitle.Year == dfF500.year)).display()
# Case sensitive for column names

# COMMAND ----------

dfF500.join(dfTitle, [dfTitle.Rank == dfF500.rank, dfTitle.Year == dfF500.year]).display()

# COMMAND ----------

# Load list into dataframe

l = [
    {'Rank': 1, 'Description': 'Gold'},
    {'Rank': 2, 'Description': 'Silver'},
    {'Rank': 3, 'Description': 'Bronze'},
]

dfTitle1 = spark.createDataFrame(l)

l = [
    {'Rank': 1, 'Description': 'Champion'},
    {'Rank': 2, 'Description': 'Silver'},
]

dfTitle2 = spark.createDataFrame(l)

dfTitle1.union(dfTitle2).display()
# Note Spark union does not remove duplicated values


# COMMAND ----------

# Load list into dataframe

l = [
    {'Rank': 1, 'Description': 'Gold'},
    {'Rank': 2, 'Description': 'Silver'},
    {'Rank': 3, 'Description': 'Bronze'},
]

dfTitle1 = spark.createDataFrame(l)

l = [
    {'Description': 'Champion', 'Rank': 1 },
    {'Description': 'Silver',   'Rank': 2 },
]

titleSchema = StructType([
    StructField("Rank",IntegerType(),True),
    StructField("Description",StringType(),True)
])

dfTitle2 = spark.createDataFrame(l, schema=titleSchema)



# COMMAND ----------

dfTitle1.display()

# COMMAND ----------

dfTitle2.display()

# COMMAND ----------

dfTitle1.union(dfTitle2).display()

# COMMAND ----------

dfTitle1.unionByName(dfTitle2).display()