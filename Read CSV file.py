# Databricks notebook source
df=spark.read.csv(path="dbfs:/FileStore/",header=True)
display(df)
df.printSchema()

# COMMAND ----------

df=spark.read.option(key="header",value=True).format("csv").load(path="dbfs:/FileStore/EmployeSample1.csv")
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType().add("id",IntegerType(),True)\
                        .add("name",StringType(),True) \
                            .add("gender",StringType(),True)\
                                .add("salary",IntegerType(),True)

df=spark.read.csv(path="dbfs:/FileStore/",schema=schema,header=True)
display(df)
df.printSchema()

# COMMAND ----------

