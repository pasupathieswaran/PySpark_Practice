# Databricks notebook source
data = [("IT",3,4),("payroll",4,5),("HR",5,6)]
schema = ["dept","male","female"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import expr

unpivotedDF = df.select("dept",expr("stack(2,'M',male,'F',female) as (gender,count)"))
unpivotedDF.show()

# COMMAND ----------

