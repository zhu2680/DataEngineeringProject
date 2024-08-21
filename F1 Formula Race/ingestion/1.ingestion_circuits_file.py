# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC ##### Step1 - read the csv file using the spark dataframe reader
# MAGIC

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/Configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions" 

# COMMAND ----------

##.. means go back to prev folder; put run in individual files

# COMMAND ----------

print(presentation_folder_path)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False), #whether nullable otr not
                                         
                                        StructField("circuitRef", StringType(), True),
                                        
                                       StructField("name", StringType(), True),  
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True), 
                                          StructField("lat", DoubleType(), True),  
                                        StructField("lng", DoubleType(), True),  
                                       
                                       StructField("alt", IntegerType(), True),
                                       StructField("url", StringType(), True)]                                                                
                                )

# COMMAND ----------


import requests,json

url ='https://ergast.com/api/f1/drivers/alonso/constructors/renault/seasons'
res = None
try:
    res = requests.get(url)
except Exception as e:
    print(e)

if res != None and res.status_code == 200:

    print(json.loads(res.text))

#circuits_df_new = 

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema() #didnot handle nulls

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select only the required columns

# COMMAND ----------

#circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location","country", "lat", "lng", "alt")

# COMMAND ----------

#circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location,circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

#circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"],circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"),col("location"),col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - rename column as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#lit convert char to column

# COMMAND ----------

circuit_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longitude")\
.withColumnRenamed("alt", "altitude")\
    .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - Add ingestion date col to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final_df  = add_ingestion_date(circuit_renamed_df)
circuit_final_df = circuit_renamed_df.withColumn("env",lit("Production"))

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - write data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md