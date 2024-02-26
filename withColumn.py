# Databricks notebook source
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

# COMMAND ----------

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col,lit

df2 = df.withColumn("salary",col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

df3 = df.withColumn("salary",col("salary")*100)
df3.printSchema()
df3.show(truncate=False) 

# COMMAND ----------

df4 = df.withColumn("CopiedColumn",col("salary")*2)
df4.show()

# COMMAND ----------

df5 = df.withColumn("country",lit("IND"))
df5.show()

# COMMAND ----------

df6 = df.withColumnRenamed("gender","sex")
df6.show()

# COMMAND ----------

df6 = df6.withColumnRenamed("sex","gender")
df6.show()

# COMMAND ----------

