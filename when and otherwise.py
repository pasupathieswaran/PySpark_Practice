# Databricks notebook source
data = [(1,"pasu","M",50000),(2,"pathi","M",50000),(3,"eswar","",100000)]
schema = ["id","name","gender","salary"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import when

df2 = df.withColumn("Full Gender",when(df.gender == "M","Male").\
    when(df.gender == "F", "Female").\
        when(df.gender.isNull(), "").\
        otherwise("No Value"))
df2.show()

# COMMAND ----------

from pyspark.sql.functions import when, col

df2 = df.select(col("*"),when(df.gender == "M","Male").\
    when(df.gender == "F", "Female").\
        when(df.gender.isNull(), "").\
        otherwise("No Value").alias("NewGender"))
df2.show()

# COMMAND ----------

