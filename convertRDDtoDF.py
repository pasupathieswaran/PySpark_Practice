# Databricks notebook source
data= [("pasu","pathi"),("om","muruga")]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
df = rdd.toDF(schema=["fn","ln"])
df.show()

# COMMAND ----------

df1 = rdd.toDF()