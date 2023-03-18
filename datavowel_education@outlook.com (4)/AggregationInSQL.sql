-- Databricks notebook source
-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val customSchema = StructType(
-- MAGIC   List(
-- MAGIC               StructField("Employee_id", IntegerType, true),
-- MAGIC               StructField("First_Name", StringType, true),
-- MAGIC               StructField("Last_Name", StringType, true),  
-- MAGIC               StructField("Gender", StringType, true),
-- MAGIC               StructField("Salary", IntegerType, true),
-- MAGIC               StructField("Date_of_Birth", StringType, true),
-- MAGIC               StructField("Age", IntegerType, true),
-- MAGIC               StructField("Country", StringType, true),
-- MAGIC               StructField("Department_id", IntegerType, true),
-- MAGIC               StructField("Date_of_Joining", StringType, true),
-- MAGIC               StructField("Manager_id", IntegerType, true),
-- MAGIC               StructField("Currency", StringType, true),
-- MAGIC               StructField("End_Date", StringType, true)
-- MAGIC 	)
-- MAGIC   
-- MAGIC )
-- MAGIC 
-- MAGIC val df = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(customSchema)
-- MAGIC .csv("/mnt/files/Employee.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.createOrReplaceTempView("viewEmployee")

-- COMMAND ----------

Select max(Department_id) from viewEmployee

-- COMMAND ----------

Select min(Department_id),avg(Department_id),mean(Department_id) from viewEmployee

-- COMMAND ----------

select count(Department_id) from viewEmployee

-- COMMAND ----------

select count(Distinct Department_id) from viewEmployee

-- COMMAND ----------

select sum(Department_id) from viewEmployee

-- COMMAND ----------

select sum( distinct Department_id) from viewEmployee

-- COMMAND ----------

select department_id
,count(distinct Employee_id) as Number_of_Employee
from viewEmployee
where department_id is not null
group by department_id
order by department_id desc

-- COMMAND ----------


