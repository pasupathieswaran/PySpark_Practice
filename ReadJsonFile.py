# Databricks notebook source
from pyspark.sql.types import *

schema = StructType().add("age",IntegerType())\
    .add("car",StringType())\
        .add("name",StringType())

df=spark.read.json(path="dbfs:/FileStore/JsonTmp/SingleLineJsonFile-1.json", schema=schema)
df.printSchema()
df.show()

# COMMAND ----------

df=spark.read.json(path="dbfs:/FileStore/JsonTmp/MultiLineJsonFile.json",multiLine=True)
df.printSchema()
df.show()

# COMMAND ----------

