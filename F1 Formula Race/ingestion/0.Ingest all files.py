# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run('1.ingestion_circuits_file',0,{"p_data_source":"Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result
#if v_result = "Success:": start next notebook

# COMMAND ----------

v_result = dbutils.notebook.run('2.ingestion_races_file',0,{"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3.Ingest_constructors_file',0,{"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4.Ingest_drivers_file',0,{"p_data_source":"Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('5.Ingest_Results_file',0,{"p_data_source":"Ergast API" , "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('6. Ingest_PitStops_file',0,{"p_data_source":"Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('7. Ingest_laptimes_file',0,{"p_data_source":"Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result = dbutils.notebook.run('8. Ingest_Qualifying_file',0,{"p_data_source":"Ergast API","p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc