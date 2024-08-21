# Databricks notebook source
# MAGIC %run ../includes/Configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Read all data as required

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.results

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_database}/races").select("race_id", "name","circuit_id", "race_timestamp","race_year").withColumnRenamed('name','race_name').withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_database}/circuits").select("circuit_id", "location").withColumnRenamed("location","circuit_location")


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_database}/drivers").select("driver_id", "name", "nationality","number").withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")


# COMMAND ----------

display(drivers_df
        )

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_database}/results").select("race_id", "driver_id", "constructor_id","grid","fastest_lap","time","points","position").withColumnRenamed("time","race_time").filter(f"file_date = '{v_file_date}'").withColumnRenamed("race_id","result_race_id").withColumn('file_date',lit(v_file_date))

# COMMAND ----------

display(results_df)

# COMMAND ----------

team_df = spark.read.parquet(f"{processed_database}/constructors").select("constructor_id", "name").withColumnRenamed("name", "team_name")

# COMMAND ----------

display(team_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = results_df.join(team_df, results_df.constructor_id == team_df.constructor_id,"inner")\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\
        .join(races_df, results_df.result_race_id == races_df.race_id, "inner")\
            .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
                .withColumn("create_date",current_timestamp())

# COMMAND ----------

final_df = final_df.drop('driver_id').drop('constructor_id').drop('result_race_id').drop('circuit_id')

# COMMAND ----------

overwirte_partition(final_df, "f1_presentation", "race_results",'race_id')
#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_result")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_presentation.race_results