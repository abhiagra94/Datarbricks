# Databricks notebook source
dfEmp = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.load("/mnt/files/Sample_data4jOIN.csv")

dfDep = spark.read.format("csv") \
.options(header='true', delimiter = ',') \
.load("/mnt/files/Department.csv")


# COMMAND ----------

from pyspark.sql.functions import col
dfEmp.join(dfDep,dfEmp.Department_id == dfDep.Department_id,"inner") \
.select(col("Employee_id"),dfEmp.Department_id,dfDep.Department_id,dfEmp.First_Name).show()

# COMMAND ----------

dfEmp.join(dfDep,dfEmp.Department_id == dfDep.Department_id,"left") \
.select(col("Employee_id"),dfEmp.Department_id,dfDep.Department_id,dfEmp.First_Name).show()

# COMMAND ----------

dfEmp.join(dfDep,dfEmp.Department_id == dfDep.Department_id,"Right") \
.select(col("Employee_id"),dfEmp.Department_id,dfDep.Department_id,dfEmp.First_Name).show()

# COMMAND ----------

dfEmp.join(dfDep,dfEmp.Department_id == dfDep.Department_id,"full") \
.select(col("Employee_id"),dfEmp.Department_id,dfDep.Department_id,dfEmp.First_Name).show()

# COMMAND ----------

dfEmp.createOrReplaceTempView("EMP")
dfDep.createOrReplaceTempView("DEPT")

# COMMAND ----------

query1 = spark.sql("Select * from EMP E INNER JOIN Dept D on E.Department_id = D.Department_id")
query1.show()

# COMMAND ----------

query2 = spark.sql("Select * from EMP E Left JOIN Dept D on E.Department_id = D.Department_id")
query2.show()

# COMMAND ----------

query3 = spark.sql("Select * from EMP E Right JOIN Dept D on E.Department_id = D.Department_id")
query3.show()

# COMMAND ----------

query4 = spark.sql("Select * from EMP E Full JOIN Dept D on E.Department_id = D.Department_id")
query4.show()

# COMMAND ----------


