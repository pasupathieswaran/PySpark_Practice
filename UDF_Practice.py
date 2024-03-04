# Databricks notebook source
data = [(1,"pasu",35000,15000),(2,"pathi",45000,18000)]
schema = ["id","name","salary","bonus"]

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import udf,upper
from pyspark.sql.types import IntegerType, StringType

@udf(returnType= IntegerType())
def totalSalary(salary,bonus):
    return salary + bonus

df.select("*",totalSalary(df.salary,df.bonus).alias("total_pay")).show()

# TotalSalary = udf(lambda s,b :totalSalary(s,b),IntegerType())

# df1 = df.withColumn("Total_Salary",TotalSalary(df.salary,df.bonus))

# df1.show()


# COMMAND ----------

data1 = [(1,"pasu",35000,15000),(2,"pathi",45000,18000)]
schema1 = ["id","name","salary","bonus"]

df1 = spark.createDataFrame(data1,schema1)
df1.createOrReplaceTempView("emps")

def totalSalary1(s,b):
    return s+b

spark.udf.register(name="TotalSalary",f= totalSalary1,returnType = IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, TotalSalary(salary,bonus) as tSalary FROM emps

# COMMAND ----------


