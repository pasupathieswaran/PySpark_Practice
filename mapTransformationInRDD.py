# Databricks notebook source
data = [("pasu","pathi"),("jaya","lakshmi")]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

def fullname(x):
    return (x[0]+" "+x[1],)

rdd1 = rdd.map(lambda x: x + fullname(x))
print(rdd1.collect())

df1 = rdd1.toDF(["fn","ln","Full Name"])
df1.show()

# COMMAND ----------

