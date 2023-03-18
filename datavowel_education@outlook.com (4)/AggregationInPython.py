# Databricks notebook source
from pyspark.sql.types import *
customSchema = StructType([
              StructField("Employee_id", IntegerType(), True),
              StructField("First_Name", StringType(), True),
              StructField("Last_Name", StringType(), True),  
              StructField("Gender", StringType(), True),
              StructField("Salary", IntegerType(), True),
              StructField("Date_of_Birth", StringType(), True),
              StructField("Age", IntegerType(), True),
              StructField("Country", StringType(), True),
              StructField("Department_id", IntegerType(), True),
              StructField("Date_of_Joining", StringType(), True),
              StructField("Manager_id", IntegerType(), True),
              StructField("Currency", StringType(), True),
              StructField("End_Date", StringType(), True)
])

# COMMAND ----------

df = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchema) \
.load("/mnt/files/Employee.csv")


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df.select(max("Department_id")).show()

# COMMAND ----------

df.select(min("Department_id")).show()
df.select(avg("Department_id")).show()
df.select(mean("Department_id")).show()

# COMMAND ----------

df.select(count("Department_id")).show()

# COMMAND ----------

df.select(countDistinct("Department_id")).show()

# COMMAND ----------

df.select(sum("Department_id")).show()

# COMMAND ----------

df.select(sumDistinct("Department_id")).show()

# COMMAND ----------

df \
    .where("Department_id is not null") \
    .groupBy("Department_id") \
    .agg(countDistinct("Employee_id")) \
    .select("Department_id",col("count(Employee_id)").alias("Number_of_Employees")) \
    .orderBy(desc("Department_id")) \
    .show()

# COMMAND ----------


