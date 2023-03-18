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

Select * from viewEmployee

-- COMMAND ----------

Select * from viewEmployee where Department_id = 1

-- COMMAND ----------

Select * from viewEmployee where department_id > 1 and Department_id < 10

-- COMMAND ----------

select * from viewEmployee where department_id between 1 and 10

-- COMMAND ----------

Select * from viewEmployee where First_Name = 'Ugo'

-- COMMAND ----------

Select * from viewEmployee where department_id is null

-- COMMAND ----------

Select * from viewEmployee where department_id is not null

-- COMMAND ----------

Select Employee_id, First_Name,Last_Name,Gender from viewEmployee

-- COMMAND ----------

Select Employee_id, First_Name,Last_Name,Gender,First_Name from viewEmployee

-- COMMAND ----------

Select Employee_id, First_Name,Last_Name,Gender,First_Name as NewColumnName from viewEmployee

-- COMMAND ----------

Select cast(Employee_id as String), First_Name,Last_Name,Gender,First_Name from viewEmployee

-- COMMAND ----------


