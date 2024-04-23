# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

data = [
    (1, "Sandeep", "Reddy"),
    (2, "Arjun", "Kumar"),
    (3, "Jhon", "Doe"),
]

df = spark.createDataFrame(data=data, schema=["id", "FirstName", "LastName"])
display(df)

# COMMAND ----------

df = df.withColumn("FullName", F.concat_ws(" ", F.col("FirstName"), F.col("LastName")))
display(df)

# COMMAND ----------

df = df.drop("FirstName", "LastName")
df.display()
