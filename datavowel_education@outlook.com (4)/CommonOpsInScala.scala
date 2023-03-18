// Databricks notebook source
import org.apache.spark.sql.types._
val customSchema = StructType(
  List(
              StructField("Employee_id", StringType, true),
              StructField("First_Name", StringType, true),
              StructField("Last_Name", StringType, true),  
              StructField("Gender", StringType, true),
              StructField("Salary", StringType, true),
              StructField("Date_of_Birth", StringType, true),
              StructField("Age", StringType, true),
              StructField("Country", StringType, true),
              StructField("Department_id", StringType, true),
              StructField("Date_of_Joining", StringType, true),
              StructField("Manager_id", StringType, true),
              StructField("Currency", StringType, true),
              StructField("End_Date", StringType, true)
	)
  
)

// COMMAND ----------

val df = spark.read
.option("header","true")
.schema(customSchema)
.csv("/mnt/files/Employee.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(col("*")).show()


// COMMAND ----------

df.select("Employee_id","First_Name","Last_Name","Gender").show()

// COMMAND ----------

df
  .withColumn("Added Column",df.col("Gender"))
  .withColumnRenamed("Added Column","Gender_New" )
  .show()


// COMMAND ----------

df
  .drop("Salary")
  .show()

// COMMAND ----------

val dfConv = df
                .withColumn("Department_id",df("Department_id").cast(IntegerType))
                .withColumn("Employee_id",df("Employee_id").cast(IntegerType))
                .withColumn("Age",df("Age").cast(IntegerType))
                .withColumn("Salary",df("Salary").cast(IntegerType))
                .withColumn("Manager_id",df("Manager_id").cast(IntegerType))

// COMMAND ----------

display(dfConv)


// COMMAND ----------

dfConv.printSchema()

// COMMAND ----------


