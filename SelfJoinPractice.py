# Databricks notebook source
data = [(1,"maheer",0),(2,"wafa",1),(3,"asi",2)]
schema = ["id","name","managerId"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col

df.alias("empdf").join(df.alias("mandf"), \
    col("empdf.managerId") == col("mandf.id"),"left").show()

# COMMAND ----------

