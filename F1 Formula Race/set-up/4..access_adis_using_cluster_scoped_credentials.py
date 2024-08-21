# Databricks notebook source
# MAGIC %md
# MAGIC ####Azure Data Lake Using Acces kEYS
# MAGIC #####julia
# MAGIC
# MAGIC 1.set the spark config fs.azure.account.key
# MAGIC 2.list files from demo container
# MAGIC 3.read data from circuits.csv

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1djulia.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1djulia.dfs.core.windows.net/circuits.csv")