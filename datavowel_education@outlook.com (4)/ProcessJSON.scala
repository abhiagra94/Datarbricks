// Databricks notebook source
val df = spark.read
.option("header","true")
.json("/mnt/files/EMPJSON.json")

// COMMAND ----------

display(df)

// COMMAND ----------

val dfFlat = df.
                select(
                        "Employee_id"
                        ,"Age"
                        ,"Customer.First_Name"
                        ,"Customer.Last_Name"
                        ,"Gender"
                        ,"Salary"
                        ,"Date_Of_Birth"
                        ,"Age"
                        ,"Country"
                        ,"Department"
                        ,"Date_Of_Joining"
                        ,"Manager_id"
                        ,"Currency"
                        ,"End_Date"
                      )

// COMMAND ----------

display(dfFlat)

// COMMAND ----------

dfFlat.createOrReplaceTempView("Emp")

// COMMAND ----------

val dfCast = spark.sql("select cast(Employee_id as int),cast(Age as int),First_Name,Last_Name,Gender,cast(Salary as int),Date_Of_Birth,cast(Age as int),Country,cast(Department as int),Date_Of_Joining,cast(Manager_id as int),Currency,End_Date from EMP")

// COMMAND ----------


