# Databricks notebook source
from pyspark.sql.types import *
customSchema = StructType([
              StructField("Employee_id", StringType(), True),
              StructField("First_Name", StringType(), True),
              StructField("Last_Name", StringType(), True),  
              StructField("Gender", StringType(), True),
              StructField("Salary", StringType(), True),
              StructField("Date_of_Birth", StringType(), True),
              StructField("Age", StringType(), True),
              StructField("Country", StringType(), True),
              StructField("Department_id", StringType(), True),
              StructField("Date_of_Joining", StringType(), True),
              StructField("Manager_id", StringType(), True),
              StructField("Currency", StringType(), True),
              StructField("End_Date", StringType(), True)
])


# COMMAND ----------

df = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchema) \
.load("/mnt/files/Employee.csv")

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("*")).show()

# COMMAND ----------

df.select("Employee_id","First_Name","Last_Name","Gender").show()

# COMMAND ----------

df \
  .drop("Salary") \
  .show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df \
     .withColumn("Department_id",col("Department_id").cast(IntegerType())) \
     .withColumn("Employee_id",col("Employee_id").cast(IntegerType())) \
     .withColumn("Salary",col("Salary").cast(IntegerType())) \
     .printSchema()
  

# COMMAND ----------

df \
  .withColumn("Added Column",col("First_Name")) \
  .show()

# COMMAND ----------

df \
  .withColumn("Added Column",col("First_Name")) \
  .withColumnRenamed("Added Column", "New Name") \
  .show()

# COMMAND ----------


