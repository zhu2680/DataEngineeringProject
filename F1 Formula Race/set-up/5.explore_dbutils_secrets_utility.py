# Databricks notebook source
# MAGIC %md
# MAGIC ##explore the capability of the dbutils.secrets.utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

dbutils.secrets.list(scope = 'Test2')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope',key='formula1julia-account-key')