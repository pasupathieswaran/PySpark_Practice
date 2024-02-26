# Databricks notebook source
data = [(1,"pasu","M",50000),(2,"pathi","M",50000),(4,"sakthi","F",200000),(4,"sakthi","F",200000)]
schema = ["id","name","gender","salary"]

df = spark.createDataFrame(data,schema)
df.show()


# COMMAND ----------

df.distinct().show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.dropDuplicates(["name"]).show()

# COMMAND ----------

df.show()

# COMMAND ----------

help(df.distinct)

# COMMAND ----------

