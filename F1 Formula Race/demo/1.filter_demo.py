# Databricks notebook source
# MAGIC %run "../includes/Configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filter_df = races_df.filter("race_year = 2019 and round<= 5")

# COMMAND ----------

races_filter_df = races_df.filter(races_df["race_year"] == 2019) &

# COMMAND ----------

display(races_filter_df)