# Databricks notebook source
# MAGIC %run ../includes/Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Inner Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id<70")\
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races__df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year == 2019")\
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

race_circuits_df = circuits_df.join(races__df,circuits_df.circuit_id == races__df.circuit_id,"inner")\
    .select(circuits_df.circuit_name,circuits_df.location, circuits_df.country,races__df.race_name,races__df.round )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Join

# COMMAND ----------

#left Outer Join
race_circuits_df = circuits_df.join(races__df,circuits_df.circuit_id == races__df.circuit_id,"left")\
    .select(circuits_df.circuit_name,circuits_df.location, circuits_df.country,races__df.race_name,races__df.round )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races__df,circuits_df.circuit_id == races__df.circuit_id,"right")\
    .select(circuits_df.circuit_name,circuits_df.location, circuits_df.country,races__df.race_name,races__df.round )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races__df,circuits_df.circuit_id == races__df.circuit_id,"full")\
    .select(circuits_df.circuit_name,circuits_df.location, circuits_df.country,races__df.race_name,races__df.round )

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### semi joins

# COMMAND ----------

# inner join but only get the column from the left dataframe

# COMMAND ----------

race_circuits_df = circuits_df.join(races__df,circuits_df.circuit_id == races__df.circuit_id,"semi")\
    .select(circuits_df.circuit_name,circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti-Join

# COMMAND ----------

#Everyhig from left dataframe not in right dataframe

# COMMAND ----------

race_circuits_df = races__df.join(circuits_df,circuits_df.circuit_id == races__df.circuit_id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cross Join

# COMMAND ----------

#Cartesina porduct
race_circuits_df = races__df.crossJoin(circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

#dimension or matrix table use case
int(races__df.count())*int(circuits_df.count())