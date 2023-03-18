-- Databricks notebook source
-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC val customSchema1 = StructType(
-- MAGIC         List(
-- MAGIC               StructField("Country", StringType, true),
-- MAGIC               StructField("Gender", StringType, true),
-- MAGIC               StructField("Employee_id", IntegerType, true),
-- MAGIC               StructField("First_Name", StringType, true),
-- MAGIC               StructField("Salary", IntegerType, true),  
-- MAGIC               StructField("Department_id", IntegerType, true)
-- MAGIC           
-- MAGIC 	))
-- MAGIC 
-- MAGIC val df = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(customSchema1)
-- MAGIC .csv("/mnt/files/SampleEmployee.csv")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.createOrReplaceTempView("viewSampleEmployee")

-- COMMAND ----------

select 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id
,Max(Salary) over (Partition by Country) as Max_Sal
,Min(Salary) over (Partition by Country) as Min_Sal
from 
viewSampleEmployee
group by 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id

-- COMMAND ----------

select 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id
,Max(Salary) over (Partition by Country) as Max_Sal
,Min(Salary) over (Partition by Country) as Min_Sal
,Row_Number() over (Partition by Country Order by Salary desc) as Row_Num
from 
viewSampleEmployee
group by 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id

-- COMMAND ----------

Select * from 
(select 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id
,Max(Salary) over (Partition by Country) as Max_Sal
,Min(Salary) over (Partition by Country) as Min_Sal
,Row_Number() over (Partition by Country Order by Salary desc) as Row_Num
from 
viewSampleEmployee ) T
where Row_Num = 3


-- COMMAND ----------

select 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id
,Rank() over (Partition by country order by Gender ) as rnk
,Dense_Rank() over (Partition by country order by Gender ) as Densernk
from 
viewSampleEmployee
group by 
Country
,Gender
,Employee_id
,First_Name
,Salary
,Department_id

-- COMMAND ----------


