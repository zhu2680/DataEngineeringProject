# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC ##### Step1 - read the csv file using the spark dataframe reader
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False), #whether nullable otr not
                                         
                                        StructField("year", IntegerType(), True),
                                        
                                       StructField("round", IntegerType(), True),  
                                        StructField("circuitId", IntegerType(), True),
                                        StructField("name", StringType(), True), 
                                          StructField("date", DateType(), True),  
                                        StructField("time", StringType(), True),
                                         StructField("url", StringType(), True), 
                                    ]                                                                
                                )

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"abfss://raw@formula1djulia.dfs.core.windows.net/{v_file_date}/races.csv")

# COMMAND ----------

races_df.printSchema() #didnot handle nulls

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp,concat,lit,current_timestamp

# COMMAND ----------

races_selected_df = races_df.drop('url')

#select(col("circuitId"), col("circuitRef"), col("name"),col("location"),col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - rename column as required

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("year", "race_year")\
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Add ingestion date col to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date",current_timestamp()).drop('time').drop('date').withColumn('data_source',lit(v_data_source))\
  .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - write data to datalake as parquet

# COMMAND ----------


races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition
# MAGIC

# COMMAND ----------

#races_final_df.write.mode('overwrite').partitionBy('race_year').parquet('abfss://processed@formula1djulia.dfs.core.windows.net/races')
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")


# COMMAND ----------

display(spark.read.parquet('abfss://processed@formula1djulia.dfs.core.windows.net/races'))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md