-- Databricks notebook source
-- MAGIC %python
-- MAGIC  # File location and type
-- MAGIC  file_location = "/mnt/files/Employee.csv"
-- MAGIC  file_type = "csv"
-- MAGIC  
-- MAGIC  # CSV options
-- MAGIC  infer_schema = "false"
-- MAGIC  first_row_is_header = "true"
-- MAGIC  delimiter = ","
-- MAGIC  
-- MAGIC  # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC  df = spark.read.format(file_type) \
-- MAGIC    .option("inferSchema", infer_schema) \
-- MAGIC    .option("header", first_row_is_header) \
-- MAGIC    .option("sep", delimiter) \
-- MAGIC    .load(file_location)
-- MAGIC  
-- MAGIC  display(df)
-- MAGIC  
-- MAGIC  temp_table_name = "employee_csv"
-- MAGIC  df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

select count(*) from employee_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/mnt/files/upsert_data.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC display(df)
-- MAGIC 
-- MAGIC # Create a view or table
-- MAGIC 
-- MAGIC temp_table_name = "upsert_data_csv"
-- MAGIC 
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------

select count(*) from upsert_data_csv

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DATAVOWELDB;
USE DATAVOWELDB

-- COMMAND ----------

DROP TABLE IF EXISTS EMPLOYEE_DELTA_TABLE;

CREATE TABLE EMPLOYEE_DELTA_TABLE
USING delta
PARTITIONED BY (DEPARTMENT_ID)
LOCATION "/DATAVOWEL/DELTA/EMPLOYEE_DATA"
AS 
  (
  SELECT * FROM EMPLOYEE_CSV WHERE DEPARTMENT_ID IS NOT NULL
  )

-- COMMAND ----------

DESCRIBE DETAIL EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

SELECT COUNT(*) FROM EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

SELECT * FROM UPSERT_DATA_CSV

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE WHERE EMPLOYEE_ID = 12

-- COMMAND ----------

-- IN EMPLOYEE_DELTA_TABLE THERE ARE 988 ROWS
--AFTER DOING MERGE OPERATION 998 ROWS WILL BE THERE AND 7 ROWS WILL BE UPDATED

-- COMMAND ----------

MERGE INTO EMPLOYEE_DELTA_TABLE                            -- the MERGE instruction is used to perform the upsert
USING upsert_data_csv

ON EMPLOYEE_DELTA_TABLE.employee_id = upsert_data_csv.employee_id -- ON is used to describe the MERGE condition
   
WHEN MATCHED THEN                                           -- WHEN MATCHED describes the update behavior
  UPDATE SET
  EMPLOYEE_DELTA_TABLE.Last_Name = upsert_data_csv.Last_Name   
WHEN NOT MATCHED THEN                                       -- WHEN NOT MATCHED describes the insert behavior
  INSERT (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)              
  VALUES (Employee_id,First_Name,Last_Name,Gender,Salary,Date_of_Birth,Age,Country,Department_id,Date_of_Joining,Manager_id,Currency,End_Date)


-- COMMAND ----------

SELECT COUNT(*) FROM EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE WHERE EMPLOYEE_ID = 12

-- COMMAND ----------

SELECT * FROM EMPLOYEE_DELTA_TABLE VERSION AS OF 1 WHERE EMPLOYEE_ID = 12

-- COMMAND ----------

DESCRIBE HISTORY EMPLOYEE_DELTA_TABLE

-- COMMAND ----------

-- MAGIC %fs ls /DATAVOWEL/DELTA/EMPLOYEE_DATA/_delta_log

-- COMMAND ----------

select * from employee_delta_table where employee_id in (21,35,45,47,49)

-- COMMAND ----------

UPDATE datavoweldb.EMPLOYEE_DELTA_TABLE SET First_Name = Null where employee_id in (21,35,45,47,49) 

-- COMMAND ----------

select * from employee_delta_table where employee_id in (21,35,45,47,49)

-- COMMAND ----------

select * from employee_delta_table version as of 1 where employee_id in (21,35,45,47,49)

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM DATAVOWELDB.employee_delta_table RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

select * from employee_delta_table version as of 2 where employee_id in (21,35,45,47,49)

-- COMMAND ----------


