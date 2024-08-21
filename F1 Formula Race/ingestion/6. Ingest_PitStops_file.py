# Databricks notebook source
# MAGIC %md
# MAGIC #### Multiline JSON Ingest PitStops File
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields = [StructField("raceId", IntegerType(), True),
                                         StructField("driverId", IntegerType(), True),
                                          StructField("stop", StringType(), True),  
                                         StructField("lap", IntegerType(), True),
                                          StructField("time", StringType(), True), 
                                          StructField("duration", StringType(), True), 
                                          StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pitstop_df = spark.read\
    .option("multiline", "true")\
    .schema(pit_stops_schema)\
        .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

display(pitstop_df) #spark by default does not deal with multiline json

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### step 2 -Rename Columns and add new columns
# MAGIC 1. rename
# MAGIC 2. add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = pitstop_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("raceId", "race_id")\
        .withColumn("ingestion_date", current_timestamp())\
            .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 write to processed container in parquet format
# MAGIC

# COMMAND ----------

#output_df = re_arrange_partition_column(final_df,'race_id')

overwirte_partition(final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")