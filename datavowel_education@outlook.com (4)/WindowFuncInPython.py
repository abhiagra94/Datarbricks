# Databricks notebook source
from pyspark.sql.types import *
customSchema = StructType([
                            StructField("Country", StringType(), True),
                            StructField("Gender", StringType(), True),
                            StructField("Employee_id", IntegerType(), True),
                            StructField("First_Name", StringType(), True),
                            StructField("Salary", IntegerType(), True),
                            StructField("Department_id", IntegerType(), True)
    
                        ])

# COMMAND ----------

df = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.schema(customSchema) \
.load("/mnt/files/SampleEmployee.csv")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

windowFuncCoun1 = Window.partitionBy("Country").orderBy(desc("Salary"))
windowFuncCounMin = Window.partitionBy("Country").orderBy(asc("Salary"))

df \
    .withColumn("MaxSalaryPerCountry",max("Salary").over(windowFuncCoun1)) \
    .withColumn("MinSalaryPerCountry",min("Salary").over(windowFuncCounMin)) \
    .withColumn("RowNumber",row_number().over(windowFuncCoun1)) \
    .where(col("RowNumber") == 3) \
    .show()

# COMMAND ----------

windowFunctionCoun2 =Window.partitionBy("Country").orderBy("Gender")

df \
    .withColumn("Rank",rank().over(windowFunctionCoun2)) \
    .withColumn("DenseRank",dense_rank().over(windowFunctionCoun2)) \
    .show()

# COMMAND ----------

windowFuncCoun3 = Window.partitionBy("Country","Gender").orderBy(desc("Salary"))

df \
    .withColumn("MaxSalaryPerCountryPerGender",max("Salary").over(windowFuncCoun3)) \
    .show()

# COMMAND ----------


