# Databricks notebook source
# MAGIC %md
# MAGIC ####Azure Data Lake Using Acces kEYS
# MAGIC #####julia
# MAGIC
# MAGIC 1.set the spark config for SAS Token
# MAGIC
# MAGIC 2.list files from demo container
# MAGIC
# MAGIC 3.read data from circuits.csv

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1djulia.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1djulia.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1djulia.dfs.core.windows.net", dbutils.secrets.get(scope="Test2", key="sassecret"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1djulia.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1djulia.dfs.core.windows.net/circuits.csv")