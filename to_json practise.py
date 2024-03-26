# Databricks notebook source
data = [("Pasupathi",{"hair":"black","eye":"brown"})]
schema= ["name","props"]
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_json
df1 = df.withColumn("Prop",to_json(df.props))
df1.show(truncate= False)

df1.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType
from pyspark.sql.functions import to_json

data1 = [("Pasupathi",("black","brown"))]
schema = StructType([\
    StructField("name",StringType()),\
        StructField("properties",StructType([ StructField("hair",StringType()),StructField("eye",StringType())\
        ]))\
])

df2 = spark.createDataFrame(data1,schema)
df2.show()
df2.printSchema()

df3 = df2.withColumn("toJsonProperties", to_json(df2.properties))
df3.show(truncate=False)
df3.printSchema()

# COMMAND ----------

