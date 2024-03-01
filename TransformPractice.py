# Databricks notebook source
data = [(1,"pasu",35000),(2,"pathi",35000)]
schema = ["id","name","salary"]

df = spark.createDataFrame(data,schema)
df.show()


# COMMAND ----------

from pyspark.sql.functions import upper

def changeNameToUpper(df):
    return df.withColumn("name",upper(df.name))
def doubleSalary(df):
    return df.withColumn("salary",df.salary *2)

df2 = df.transform(changeNameToUpper).transform(doubleSalary)
df2.show()

# COMMAND ----------

data1 = [(1,"pasu",["sql","python"]),(2,"pathi",["adf","adb"])]
schema = ["id","name","skills"]
dfs = spark.createDataFrame(data1,schema)

from pyspark.sql.functions import transform,upper
dfs.select("*",transform("skills",lambda x: upper(x)).alias("UpperCase Skills")).show()

# COMMAND ----------


