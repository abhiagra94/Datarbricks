// Databricks notebook source
// MAGIC %md
// MAGIC #Filters in scala
// MAGIC * Demonstration by using multiple approaches

// COMMAND ----------

import org.apache.spark.sql.types._
val customSchema = StructType(
  List(
              StructField("Employee_id", IntegerType, true),
              StructField("First_Name", StringType, true),
              StructField("Last_Name", StringType, true),  
              StructField("Gender", StringType, true),
              StructField("Salary", IntegerType, true),
              StructField("Date_of_Birth", StringType, true),
              StructField("Age", IntegerType, true),
              StructField("Country", StringType, true),
              StructField("Department_id", IntegerType, true),
              StructField("Date_of_Joining", StringType, true),
              StructField("Manager_id", IntegerType, true),
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

display(df)

// COMMAND ----------

df.filter($"Department_id" === 1).show()

// COMMAND ----------

df.filter("Department_id == 1").show()

// COMMAND ----------

df.filter($"Department_id" =!= 1).show()

// COMMAND ----------

df.filter($"Department_id" > 1 and $"Department_id" < 10).show()
df.where($"Department_id" > 1 and $"Department_id" < 10).show()

// COMMAND ----------

df.filter($"Department_id" >1)
.filter($"Department_id" < 10)
.show()

// COMMAND ----------

df.where($"Department_id" >1)
.where($"Department_id" < 10)
.show()

// COMMAND ----------

df.filter($"First_Name" === "Ugo").show()

// COMMAND ----------

import org.apache.spark.sql.functions._
df.filter(df("Department_id") === 1).show()

// COMMAND ----------

df.filter(col("Department_id") === 1).show()

// COMMAND ----------

df.filter(col("Department_id").isNull).show()

// COMMAND ----------

df.filter(col("Department_id").isNotNull).show()

// COMMAND ----------

df
  .withColumn("Added_Column",df.col("First_Name"))
  .withColumnRenamed("Added_Column","Renamed_Column")
  .printSchema()

// COMMAND ----------

df
  .drop("Last_Name")
  .printSchema()

// COMMAND ----------

import org.apache.spark.sql.types.IntegerType
val dfEmpConv = df
                .withColumn("Department_id",df("Department_id").cast(IntegerType))
                .withColumn("Employee_id",df("Employee_id").cast(IntegerType))
                .withColumn("Age",df("Age").cast(IntegerType))
                .withColumn("Salary",df("Salary").cast(IntegerType))
                .withColumn("Manager_id",df("Manager_id").cast(IntegerType))

dfEmpConv.printSchema()

