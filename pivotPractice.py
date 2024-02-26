# Databricks notebook source
data = [(1,"pasu","M",50000,"DE"),(2,"pathi","M",50000,"DE"),(3,"eswar","M",100000,"DS"),(4,"shakthi","F",20000,"DA"),(5,"lakshmi","F",20000,"DA")]
schema = ["id","name","gender","salary","dept"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.groupBy("dept").pivot("gender",["M"]).count().show()

# COMMAND ----------

df.groupBy("dept").pivot("gender").count().show()

# COMMAND ----------



# COMMAND ----------

