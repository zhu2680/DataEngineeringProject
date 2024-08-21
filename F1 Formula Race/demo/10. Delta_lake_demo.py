# Databricks notebook source
# MAGIC %md
# MAGIC 1. write data to delta lake (managed table)
# MAGIC 2. write data to delta lake (external table)
# MAGIC 3. read data from delta lake(table)
# MAGIC 4. read data from delta lake (`file`)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create database if not exists f1_demo
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ../includes/Configuration

# COMMAND ----------

results_df = spark.read\
    .option("inferSchema",True)\
        .json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{demo_path}/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_demo.results_external

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location "abfss://demo@formula1djulia.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended f1_demo.results_external

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load(f"{demo_path}/results_external")

# COMMAND ----------

results_external_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. update delta table
# MAGIC 2. delete from delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11-position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,f"{demo_path}/results_external")
deltaTable.update("position <= 10",{"position": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark,f"{demo_path}/results_external")
deltaTable.delete("position <= 10")

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read\
    .option('inferSchema', 'true')\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId <= 10")\
                .select("driverId", "dob", 'name.forename','name.surname')

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read\
    .option('inferSchema', 'true')\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId between 6 and 15")\
                .select("driverId", "dob", upper('name.forename').alias("forename"),upper('name.surname').alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read\
    .option('inferSchema', 'true')\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId between 1 and 5 or driverId between 16 and 20")\
                .select("driverId", "dob", upper('name.forename').alias("forename"),upper('name.surname').alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_demo.drivers_merge;
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using drivers_day1 upd
# MAGIC on tgt.driver_id = upd.driver_id
# MAGIC when matched then
# MAGIC  update set tgt.dob = upd.dob,
# MAGIC  tgt.forename = upd.forename,
# MAGIC  tgt.surname = upd.surname
# MAGIC when not matched then 
# MAGIC insert(driverId, dob,forename,surname,createdDate) values (date,)