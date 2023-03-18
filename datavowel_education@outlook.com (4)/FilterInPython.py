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

df.filter("Department_id == 1").show()

# COMMAND ----------

df.where("Department_id == 1").show()

# COMMAND ----------

df.filter("Department_id != 1").show()

# COMMAND ----------

df.filter("Department_id > 1" and "Department_id < 10").show()

# COMMAND ----------

df.where("Department_id > 1" and "Department_id < 10").show()

# COMMAND ----------

df.filter("Department_id > 1") \
.filter("Department_id < 10") \
.show()


# COMMAND ----------

df.filter("First_Name == 'Ugo'").show()

# COMMAND ----------

from pyspark.sql.functions import col
##df.filter("Department_id == 1").show()
df.filter(df.Department_id == 1).show()

# COMMAND ----------

df.filter(col("Department_id") == 1).show()

# COMMAND ----------

df.filter(df.Department_id.isNull()).show()

# COMMAND ----------

df.filter(df.Department_id.isNotNull()).show()

# COMMAND ----------


