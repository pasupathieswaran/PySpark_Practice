# Databricks notebook source
data1 = [(1,"pasu",30)]
data2 = [(2,"pathi",3000)]

schema1 = ["id","name","age"]
schema2 = ["id","name","salary"]

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)
df1.union(df2).show()
df3 = df1.unionByName(df2,allowMissingColumns =True)
df3.show()

# COMMAND ----------

