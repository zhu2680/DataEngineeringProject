# Databricks notebook source
# MAGIC %md
# MAGIC ####Access dataframe using SQL
# MAGIC #####Objectives
# MAGIC 1. create temporay views on dataframes
# MAGIC 2. access the view from sql cell
# MAGIC 3. accessthe view from python cell

# COMMAND ----------

# MAGIC %run "../includes/Configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

# COMMAND ----------

race_results_df.createOrReplaceTempView("j_race_results") #only exist ithin the notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(points) as total_points 
# MAGIC from j_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

#python cell allow variable while sql cell do not

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_results_df_2019 = spark.sql(f"select * from j_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_df_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Access dataframe using SQL
# MAGIC #####Objectives
# MAGIC 1. create global temporay views on dataframes
# MAGIC 2. access the view from sql cell
# MAGIC 3. access the view from python cell
# MAGIC 4. Access the view from another workbook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * \
    from global_temp.gv_race_results").show()