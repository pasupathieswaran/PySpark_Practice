# Databricks notebook source
data = [(1,"pasu","M",50000),(2,"pathi","M",50000),(3,"eswar","M",100000),(4,"sakthi","F",200000)]
schema = ["id","name","gender","salary"]

df = spark.createDataFrame(data,schema)
# df.sort(df.name.asc()).show()
df1 = df.select(df.id,df.name,df.salary.cast("int"))
df1.printSchema()
df.filter(df.name.like("p%")).show()

# COMMAND ----------

