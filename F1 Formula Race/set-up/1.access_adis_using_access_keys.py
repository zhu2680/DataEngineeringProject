# Databricks notebook source
# MAGIC %md
# MAGIC ####Azure Data Lake Using Acces kEYS
# MAGIC #####julia
# MAGIC
# MAGIC 1.set the spark config fs.azure.account.key
# MAGIC 2.list files from demo container
# MAGIC 3.read data from circuits.csv

# COMMAND ----------

dbutils.secrets.get(scope = "formula1-scope", key = "formula1julia-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1djulia.dfs.core.windows.net",
    "qGU5xDTpShYlLhosfSZw3xqp3AHp0r7z1NO3lUwjYEFUH72ZJcE1vgyfq5HnZpqIXlGNBD5z2mXl+AStxWtVzg=="
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1djulia.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://demo@formula1djulia.dfs.core.windows.net/circuits.csv")