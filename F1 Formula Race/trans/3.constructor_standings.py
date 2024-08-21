# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_database}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_database}/race_results")\
  .filter(f"file_date = '{v_file_date}'") 


race_year_list = df_column_to_lists(race_results_df,'race_year')


# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_database}/race_results")\
  .filter(col("race_year").isin(race_year_list))

# COMMAND ----------



constructor_standings_df = race_results_df\
    .groupBy("race_year", "team_name")\
        .agg(sum("points").alias("total_points"), 
            count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points")).orderBy(desc("wins"))
final_df =constructor_standings_df.withColumn("constructor_rank",rank().over(constructor_rank_spec))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists  f1_presentation.constructor_standings

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
overwirte_partition(final_df,'f1_presentation','constructor_standings','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC hive_metastore.f1_presentation.race_resultsdescribe extended f1_presentation.constructor_standings