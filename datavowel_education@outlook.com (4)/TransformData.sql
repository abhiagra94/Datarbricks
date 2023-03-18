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
-- MAGIC  )
-- MAGIC 
-- MAGIC val df = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(customSchema)
-- MAGIC .csv("/mnt/files/Employee.csv")
-- MAGIC 
-- MAGIC 
-- MAGIC val depSchema = StructType(
-- MAGIC   List(
-- MAGIC               StructField("Department_id", IntegerType, true),
-- MAGIC               StructField("Name", StringType, true)
-- MAGIC               
-- MAGIC 	)
-- MAGIC   
-- MAGIC )
-- MAGIC 
-- MAGIC val dfD = spark.read
-- MAGIC .option("header","true")
-- MAGIC .schema(depSchema)
-- MAGIC .csv("/mnt/files/Department.csv")
-- MAGIC 
-- MAGIC   

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC df.createOrReplaceTempView("Emp")
-- MAGIC dfD.createOrReplaceTempView("Dept")

-- COMMAND ----------

select count(*) from Emp E left join Dept D on E.Department_id = D.Department_id where E.Department_id is not null --and D.Department_id is not null --where D.Department_id is not null

-- COMMAND ----------

select count(*) from Emp where department_id is null

-- COMMAND ----------

select avg(Salary) over (Partition by E.Department_id)
from Emp E 
left join Dept D on E.Department_id = D.Department_id where E.Department_id is not null

-- COMMAND ----------

--val bdf = spark.sql("""select from_unixtime(unix_timestamp(Id, 'MM-dd-yyyy')) as new_format from table1""")

-- COMMAND ----------

--TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))

-- COMMAND ----------

select from_unixtime(unix_timestamp(Date_of_birth, 'MM-dd-yyyy')) as new_format from emp

-- COMMAND ----------

SELECT D.Name, 
         AVG(salary) AS avg_salary,
         STD(salary) AS std_salary,
         MAX(salary) AS max_salary 
  FROM Emp E  
  left join Dept D on E.Department_id = D.Department_id where D.Name is not null
  GROUP BY D.Name

-- COMMAND ----------

SELECT date_of_birth, date_of_joining, 
TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)) as db,
TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) as dj,
TO_DATE(CAST(UNIX_TIMESTAMP(current_date, 'dd/MM/yyyy') AS TIMESTAMP)) as dt,
  DATEDIFF( TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS diff_days,
  CAST( months_between( TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS INT )/12 AS diff_months,
  CAST( months_between( TO_DATE(CAST(UNIX_TIMESTAMP(current_date, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS INT )/12 AS aging,
  CAST( months_between( TO_DATE(CAST(UNIX_TIMESTAMP(current_date, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS INT )/12 AS correct_age,
  age
 FROM emp
;

-- COMMAND ----------

select * from 
(select
E.department_id
,Name
,employee_id
,row_number() over(partition by E.department_id order by salary desc )rw
,salary
FROM emp E 
left join
dept d
on E.department_id = d.Department_id
where E.department_id is not null and d.name is not null) where rw < 6

-- COMMAND ----------

select
Name,Country
,sum(salary)/1000000 as cost_of_department_in_million
FROM emp E 
left join
dept d
on E.department_id = d.Department_id
where Name is not null
group by 
Name,Country


-- COMMAND ----------

select
Name,Country
,sum(salary) as cost_of_department_in_million
,case 
    when country = "Australia" then "AUD" 
    when country = "Brazil" then "BRA" 
    when country = "Canada" then "CAN" 
    when country = "China" then "CHN" 
    when country = "Denmark" then "DNK" 
    when country = "Germany" then "DEU" 
    when country = "India" then "IND" 
    when country = "Japan" then "JPN" 
    when country = "Sweden" then "SWE" 
    when country = "UAE" then "ARE" 
    when country = "USA" then "USA" 
  end as code
FROM emp E 
left join
dept d
on E.department_id = d.Department_id
where Name is not null
group by 
Name,Country


-- COMMAND ----------

select

case 
    when country = "Australia" then "AUS"  ----- ISO 3166-1 alpha-3 code
    when country = "Brazil" then "BRA"     ----- ISO 3166-1 alpha-3 code
    when country = "Canada" then "CAN"     ----- ISO 3166-1 alpha-3 code
    when country = "China" then "CHN"      ----- ISO 3166-1 alpha-3 code
    when country = "Denmark" then "DNK"    ----- ISO 3166-1 alpha-3 code
    when country = "Germany" then "DEU"    ----- ISO 3166-1 alpha-3 code
    when country = "India" then "IND"      ----- ISO 3166-1 alpha-3 code
    when country = "Japan" then "JPN"      ----- ISO 3166-1 alpha-3 code
    when country = "Sweden" then "SWE"     ----- ISO 3166-1 alpha-3 code
    when country = "UAE" then "ARE"        ----- ISO 3166-1 alpha-3 code
    when country = "USA" then "USA"        ----- ISO 3166-1 alpha-3 code
  end as code
 , sum(salary) as cost_of_department_in_million
FROM emp E 
left join
dept d
on E.department_id = d.Department_id
where Name is not null
group by 
code


-- COMMAND ----------

SELECT D.Name, 
         AVG(salary) AS avg_salary,
         STD(salary) AS std_salary,
         MAX(salary) AS max_salary 
  FROM Emp E  
  left join Dept D on E.Department_id = D.Department_id where D.Name is not null
  GROUP BY D.Name

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = sc.parallelize(Seq("08-26-2016")).toDF("Id")
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("table1")
-- MAGIC 
-- MAGIC val bdf = spark.sql("""select from_unixtime(unix_timestamp(Id, 'MM-dd-yyyy')) as new_format from table1""")
-- MAGIC 
-- MAGIC bdf.printSchema
-- MAGIC 
-- MAGIC bdf.show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df = sc.parallelize(Seq("08-26-2016")).toDF("Id")
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("table1")
-- MAGIC val bdf = spark.sql("""select from_unixtime(unix_timestamp(Id, 'MM-dd-yyyy')) as new_format from table1""")
-- MAGIC bdf.printSchema
-- MAGIC bdf.show
-- MAGIC 
-- MAGIC val bbdf = bdf.withColumn("dt",$"new_format".cast("date"))
-- MAGIC bbdf.printSchema
-- MAGIC bbdf.show

-- COMMAND ----------

--TO_DATE(CAST(UNIX_TIMESTAMP(date, 'MM/dd/yyyy') AS TIMESTAMP))

-- COMMAND ----------

SELECT date_of_birth, date_of_joining, 
TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)) as db,
TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) as dj,
  DATEDIFF( TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS diff_days,
  CAST( months_between( TO_DATE(CAST(UNIX_TIMESTAMP(date_of_birth, 'dd/MM/yyyy') AS TIMESTAMP)), TO_DATE(CAST(UNIX_TIMESTAMP(date_of_joining, 'dd/MM/yyyy') AS TIMESTAMP)) ) AS INT )/12 AS diff_months  
 FROM emp
;

-- COMMAND ----------


