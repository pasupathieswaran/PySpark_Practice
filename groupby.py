# Databricks notebook source
data = [(1,"pasu","M",50000),(2,"pathi","M",50000),(3,"eswar","M",100000),(4,"shakthi","F",20000),(5,"lakshmi","F",20000)]
schema = ["id","name","gender","salary"]

df = spark.createDataFrame(data,schema)
df.show()



# COMMAND ----------

from pyspark.sql.functions import count,min,max
df1 = df.groupBy("gender").agg(count("*"),max(df.salary))
df1.show()

# COMMAND ----------

