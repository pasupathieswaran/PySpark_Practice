# Databricks notebook source
df = spark.range(1,101)

df1 = df.sample(fraction= 0.1, seed = 12)
df2 = df.sample(fraction= 0.1, seed = 1)
df1.display()
df2.display()

# COMMAND ----------


