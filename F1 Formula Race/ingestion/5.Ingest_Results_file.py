# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

spark.read\
        .json('abfss://raw@formula1djulia.dfs.core.windows.net/2021-03-21/results.json').createOrReplaceTempView('results_cutover')

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from results_cutover
# MAGIC group by  raceId
# MAGIC order by raceId Desc

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(),True),
                                     StructField("driverId", IntegerType(),True),
                                     StructField("constructorId", IntegerType(),True),
                                   
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(),True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(),True),
                                    StructField("positionOrder", IntegerType(),True),
                                   StructField("points", FloatType(),True),
                                   StructField("laps", IntegerType(),True),
                                   StructField("time", StringType(),True),
                                   StructField("milliseconds", IntegerType(),True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True), 
                                    StructField("fastestLapTime", StringType(), True),     
                                    StructField("fastestLapSpeed", FloatType(), True),                                 StructField("statusId", IntegerType(), True)]) 

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
        .json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step2 -Rename columns and add new columns
# MAGIC 1. rename
# MAGIC 2. ingestion date
# MAGIC 3. name concat add

# COMMAND ----------

from pyspark.sql.functions import col, concat,current_timestamp,lit


# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "postion_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("FastestLapSpeed", "fastest_lap_speed").withColumn("ingestion_date", current_timestamp()).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - drop unwanted columns from the dataframe 
# MAGIC
# MAGIC 1. url

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_renamed_df.drop(col('statusId'))
#constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 Write output to parquet File

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Method 1
# MAGIC ######delete all entries for it race_id if it's wrong first itme
# MAGIC ######never do collect on database

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
   # if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
      #  spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------


#results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2 -more efficient

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results

# COMMAND ----------

output_df = re_arrange_partition_column(results_final_df,'race_id')

# COMMAND ----------

  overwirte_partition(results_final_df,'f1_processed','results', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")