# Databricks notebook source
data = [(1,"Pasu"),(2,"pathi")]
schema = ["id","name"]
df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

type(df.write)

# COMMAND ----------

df.write.csv(path="dbfs:/tmp/emps",header=True)

# COMMAND ----------

df.write.csv("dbfs:/tmp1/emps",header=True)

# COMMAND ----------

display(spark.read.csv("dbfs:/tmp1/emps"))

# COMMAND ----------

