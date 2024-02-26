# Databricks notebook source
data1 = [(1,"Pasu",5000),(2,"pathi",10000),(3,"eswaran",15000)]
data2 = [(3,"eswaran",15000),(4,"Siva",5000),(5,"shakthi",10000),(6,"sivaSakthi",15000)]

df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)

df3 = df1.unionAll(df2)
df3.distinct().show()

# COMMAND ----------

df4 = df1.unionByName(df2, allowMissingColumns =True)
df4.show()