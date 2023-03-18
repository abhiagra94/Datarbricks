-- Databricks notebook source
-- MAGIC %scala
-- MAGIC val dfEmp = spark.read
-- MAGIC .option("header","true")
-- MAGIC .csv("/mnt/files/Sample_data4jOIN.csv")
-- MAGIC 
-- MAGIC //Creating the dataframe by name dfDep
-- MAGIC val dfDep = spark.read
-- MAGIC .option("header","true")
-- MAGIC .csv("/mnt/files/Department.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dfEmp.createOrReplaceTempView("Emp")
-- MAGIC dfDep.createOrReplaceTempView("Dept")

-- COMMAND ----------

Select
Dept.Department_id
,Emp.Department_id
,Dept.Name
from
EMP
INNER JOIN
DEPT
on
EMP.Department_id = Dept.Department_id

-- COMMAND ----------

Select
Emp.Department_id
,Dept.Name
,Dept.Department_id
from
EMP
Left JOIN
DEPT
on
EMP.Department_id = Dept.Department_id

-- COMMAND ----------

Select
Emp.Department_id
,Dept.Name
,Dept.Department_id
from
EMP
Right JOIN
DEPT
on
EMP.Department_id = Dept.Department_id

-- COMMAND ----------

Select
Emp.Department_id
,Dept.Name
,Dept.Department_id
from
EMP
Full JOIN
DEPT
on
EMP.Department_id = Dept.Department_id

-- COMMAND ----------


