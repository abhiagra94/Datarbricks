// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructField}

val customSchema1 = StructType(
        List(
              StructField("Country", StringType, true),
              StructField("Gender", StringType, true),
              StructField("Employee_id", IntegerType, true),
              StructField("First_Name", StringType, true),
              StructField("Salary", IntegerType, true),  
              StructField("Department_id", IntegerType, true)
          
	))

val df = spark.read
.option("header","true")
.schema(customSchema1)
.csv("/mnt/files/SampleEmployee.csv")
df.show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowFuncCoun1 = Window.partitionBy("Country").orderBy(desc("Salary"))
val windowFuncCounMin = Window.partitionBy("Country").orderBy(asc("Salary"))

df
  .withColumn("MaxSalaryPerCountry",max("Salary").over(windowFuncCoun1))
  .withColumn("MinSalaryPerCountry",min("Salary").over(windowFuncCounMin))
  .withColumn("RowNumber",row_number().over(windowFuncCoun1))
  .where(col("RowNumber") === 4)
  .select("*")
  .show()




// COMMAND ----------

val windowFunctionCoun2 = Window.partitionBy("Country").orderBy("Gender")

df
  .withColumn("Rank",rank().over(windowFunctionCoun2))
  .withColumn("DenseRank",dense_rank().over(windowFunctionCoun2))
  .show()

// COMMAND ----------

val windowFuncCoun3 = Window.partitionBy("Country","Gender").orderBy(desc("Salary"))

df
  .withColumn("MaxSalaryPerCountryPerGender",max("Salary").over(windowFuncCoun3))
  .show()

// COMMAND ----------


