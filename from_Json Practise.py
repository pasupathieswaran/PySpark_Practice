# Databricks notebook source
data = [("Pasupathi",'{"hair":"black","eye":"brown"}')]
schema= ["name","props"]
df = spark.createDataFrame(data,schema)
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType
PropsSchema = MapType(StringType(),StringType())
df1 = df.withColumn("Properties",from_json(df.props,PropsSchema))
df2 = df1.withColumn("hair",df1.Properties.hair)\
    .withColumn("eye",df1.Properties.eye)
df1.show(truncate=False)
df2.show(truncate=False)
df1.printSchema()


# COMMAND ----------

data = [("Pasupathi",'{"hair":"black","eye":"brown"}')]
schema= ["name","props"]
df = spark.createDataFrame(data,schema)
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructField,StructType, StringType
StructSchema= StructType([
    StructField("hair",StringType()),\
        StructField("eye",StringType()),
])
df1 = df.withColumn("propsStruct",from_json(df.props,StructSchema))
df1.show(truncate=False)
df2 = df1.withColumn("hair",df1.propsStruct.hair)\
    .withColumn("eye",df1.propsStruct.eye)
df2.show(truncate=False)

# COMMAND ----------

