-- Databricks notebook source
create table
datavoweldb.Managed_employee
(
      Employee_id INT comment "Stores unique employee id",
      First_Name STRING comment "Stores First name of employee",
      Last_Name STRING ,  
      Gender STRING,
      Salary INT,
      Date_of_Birth STRING,
      Age INT,
      Country STRING,
      Department_id INT,
      Date_of_Joining STRING,
      Manager_id INT,
      Currency STRING,
      End_Date STRING 
)

-- COMMAND ----------

select * from datavoweldb.Managed_employee

-- COMMAND ----------

create table if not exists
datavoweldb.employee
(
      Employee_id INT,
      First_Name STRING,
      Last_Name STRING ,  
      Gender STRING,
      Salary INT,
      Date_of_Birth STRING,
      Age INT,
      Country STRING,
      Department_id INT,
      Date_of_Joining STRING,
      Manager_id INT,
      Currency STRING,
      End_Date STRING 
)
using csv
options (
path '/mnt/files/Employee.csv',
sep ',',
header true
)

-- COMMAND ----------

INSERT INTO datavoweldb.Managed_employee
SELECT
Employee_id
,First_Name
,Last_Name
,Gender
,Salary
,Date_of_Birth
,Age
,Country
,Department_id
,Date_of_Joining
,Manager_id
,Currency
,End_Date
FROM 
datavoweldb.employee

-- COMMAND ----------

select * from datavoweldb.managed_employee

-- COMMAND ----------

describe formatted datavoweldb.managed_employee

-- COMMAND ----------

-- MAGIC %fs ls user/hive/warehouse/datavoweldb.db/managed_employee

-- COMMAND ----------

drop table datavoweldb.managed_employee

-- COMMAND ----------

-- MAGIC %fs ls user/hive/warehouse/datavoweldb.db/managed_employee

-- COMMAND ----------


