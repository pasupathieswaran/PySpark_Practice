# Databricks notebook source
data = [(1,"pasu"),(2,"pathi")]
schema = ["id","name"]

df = spark.createDataFrame(data,schema)

df.show()

# COMMAND ----------

df.createOrReplaceTempView("sample")
df.createOrReplaceGlobalTempView("glbData")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM global_temp.glbdata

# COMMAND ----------

spark.catalog.listTables("global_temp")

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables("default")

# COMMAND ----------

