# Databricks notebook source
df = spark.read.parquet("dbfs:/FileStore/*.parquet")
display(df)
print("Number of rows in this file :"+ str(df.count()))