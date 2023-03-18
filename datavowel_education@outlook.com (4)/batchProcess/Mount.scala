// Databricks notebook source
val containerName = "landing"
val storageAccountName = "datavowelstorage"
val sas = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-03-02T06:09:02Z&st=2021-02-21T22:09:02Z&spr=https&sig=wBDVJfKpXSdsnglDaNiVCs%2Bs1epTD7TvIgvpkv8288c%3D"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/landing",
extraConfigs = Map(config -> sas))

// COMMAND ----------

val containerName = "archive"
val storageAccountName = "datavowelstorage"
val sas = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-03-02T06:09:02Z&st=2021-02-21T22:09:02Z&spr=https&sig=wBDVJfKpXSdsnglDaNiVCs%2Bs1epTD7TvIgvpkv8288c%3D"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/archive",
extraConfigs = Map(config -> sas))

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS DATAWAREHOUSEDB;

// COMMAND ----------


