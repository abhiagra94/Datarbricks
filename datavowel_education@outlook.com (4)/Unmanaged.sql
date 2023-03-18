-- Databricks notebook source
create database datavoweldb;

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

select * from datavoweldb.employee

-- COMMAND ----------

describe formatted datavoweldb.employee

-- COMMAND ----------

drop table datavoweldb.employee;

-- COMMAND ----------

-- MAGIC %fs ls /mnt/files

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

drop table datavoweldb.employee

-- COMMAND ----------

-- MAGIC %fs ls /mnt/files

-- COMMAND ----------


