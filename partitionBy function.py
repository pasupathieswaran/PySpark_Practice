# Databricks notebook source
data = [("Pasupathi", "male","IT"),("Velu", "male","IT"),("Jaya", "Female","HR"),("Sree", "Female","IT"),("Siva", "Female","HR")]
schema = ["name","gender","dept"]
df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.write.parquet("/FileStore/employee1",mode="overwrite",partitionBy=["dept","gender"])

# COMMAND ----------

spark.read.parquet("/FileStore/employee1").show()

# COMMAND ----------

