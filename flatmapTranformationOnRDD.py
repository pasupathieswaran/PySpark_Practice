# Databricks notebook source
data = [("Pasu pathi"),("Jaya lakshmi")]
rdd = spark.sparkContext.parallelize(data)
for item in rdd.collect():
    print(item)

# COMMAND ----------

rdd2 = rdd.map(lambda x : x.split(" "))
for item in rdd2.collect():
    print(item)

# COMMAND ----------

rdd3 = rdd.flatMap(lambda x: x.split(" "))
for item in rdd3.collect():
    print(item)

# COMMAND ----------

