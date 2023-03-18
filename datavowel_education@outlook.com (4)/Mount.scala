// Databricks notebook source
val containerName = "files"
val storageAccountName = "datavowelstorage"
val sas = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-03-02T06:09:02Z&st=2021-02-21T22:09:02Z&spr=https&sig=wBDVJfKpXSdsnglDaNiVCs%2Bs1epTD7TvIgvpkv8288c%3D"
val url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
var config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// COMMAND ----------

dbutils.fs.mount(
source = url,
mountPoint = "/mnt/files",
extraConfigs = Map(config -> sas))

// COMMAND ----------

// MAGIC %fs ls /mnt/files
