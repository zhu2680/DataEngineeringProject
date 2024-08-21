# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")  #default
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#define names field
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True), StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(),True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),                                    StructField("url", StringType(), True)]) 

# COMMAND ----------

drivers_df = spark.read\
    .schema(driver_schema)\
        .json(f'abfss://raw@formula1djulia.dfs.core.windows.net/{v_file_date}/drivers.json')

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step2 -Rename columns and add new columns
# MAGIC 1. rename
# MAGIC 2. ingestion date
# MAGIC 3. name concat add

# COMMAND ----------

from pyspark.sql.functions import col, concat,current_timestamp,lit


# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
        .withColumn("ingestin_date", current_timestamp())\
            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - drop unwanted columns from the dataframe 
# MAGIC 1. forename and last name (already gone)
# MAGIC 2. url

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col('url'))
#constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 4 Write output to parquet File

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers