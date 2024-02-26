# Databricks notebook source
from pyspark.sql.types import StructType,StructField ,IntegerType, ArrayType,StringType

data = [(1,"Pasu",[1,2,3]),(2,"pathi",[4,5,6]),(3,"eswaran",[7,8,9])]
schema = StructType([\
    StructField("id",IntegerType()),\
        StructField("name",StringType()),\
            StructField("numbers",ArrayType(IntegerType()))
])

df = spark.createDataFrame(data,schema)

df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.functions import array

data1 = [(1,2),(3,4)]
schema = ["num1","num2"]

df1= spark.createDataFrame(data1,schema)
df1.show()
df2 = df1.withColumn("Numbers",array("num1", "num2"))

df2.show()

# COMMAND ----------

from pyspark.sql.functions import explode,col
df5=df.withColumn("AllCOls",explode(df.numbers))
df5.show()

# COMMAND ----------

from pyspark.sql.functions import explode,col 

help(explode)

# COMMAND ----------

from pyspark.sql.functions import split
data = [(1,"aqw,brty,chj"),(2,"wbn,rdfg,tsfg,yjki")]
schema = ["id","letters"]

splitsDF = spark.createDataFrame(data,schema)
newDf = splitsDF.withColumn("letters", split("letters",","))
display(newDf)
newDf.printSchema()

# COMMAND ----------

