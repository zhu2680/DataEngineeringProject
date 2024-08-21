# Databricks notebook source
# MAGIC %md
# MAGIC #### Multi file csv Ingest laptimes File
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId", IntegerType(), True),
                                         StructField("driverId", IntegerType(), True),
                                            
                                         StructField("lap", IntegerType(), True),
                                         StructField("position", IntegerType(), True),
                                          StructField("time", StringType(), True), 
                                          StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(lap_times_schema)\
        .csv(f'{raw_folder_path}/{v_file_date}/lap_times')

'''or lap_times_df = spark.read\
    .schema(lap_times_schema)\
        .csv('abfss://raw@formula1djulia.dfs.core.windows.net/lap_times/lap_times_split*') in case there is other file in the folder
'''

# COMMAND ----------

display(lap_times_df) #spark by default does not deal with multiline json

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### step 2 -Rename Columns and add new columns
# MAGIC 1. rename
# MAGIC 2. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("raceId", "race_id")\
        .withColumn("ingestion_date", current_timestamp())\
            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 write to processed container in parquet format
# MAGIC

# COMMAND ----------

overwirte_partition(final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")