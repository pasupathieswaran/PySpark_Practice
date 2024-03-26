# Databricks notebook source
data = [(1,"pasu",35000),(2,"pathi",35000)]
schema = ["id","name","salary"]

df = spark.createDataFrame(data,schema)
df.show()

dataRows = df.collect()
print(dataRows[0])

# COMMAND ----------

print(dataRows[0][1])

# COMMAND ----------

