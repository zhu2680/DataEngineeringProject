# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructors_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - drop unwanted columns from the dataframe 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))
#constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename Columns and add ingestion date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

construction_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
        .withColumn('ingestion_date', current_timestamp())\
            .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(construction_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 Write output to parquet File

# COMMAND ----------


construction_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")