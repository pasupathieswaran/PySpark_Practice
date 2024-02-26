# Databricks notebook source
from pyspark.sql.types import *
data = [(1,"Pasu"),(2,"pathi")]
schema = StructType([StructField("id",IntegerType(),True),
                    StructField("name",StringType())])
df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

data = [{"id":1,"name":"Pasu"},
        {"id":2,"name":"pathi"}]

df = spark.createDataFrame(data=data)
df.show()
df.printSchema()

# COMMAND ----------

