# Databricks notebook source
from pyspark.sql.functions import lit

help(lit)

# COMMAND ----------

from pyspark.sql.functions import lit, col
data1 = [("pasu","male",5000),("pathi","male",50000)]
schema = ["name","gender","salary"]
df = spark.createDataFrame(data1,schema)
df1 = df.withColumn("newCol",lit("MyNewCol"))

df1.select(df1.name, df1["gender"]).show()
df1.select(df1["gender"]).show()
df1.select(col("salary")).show()

df1.show()
df1.printSchema()

# COMMAND ----------

