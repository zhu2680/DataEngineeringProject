-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. data tab in the UI
-- MAGIC 4. SHOW Command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. find the current database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Managed Table
-- MAGIC 1. create managed tablee using python  and sql
-- MAGIC 2. effect of dropping managed table
-- MAGIC 3. describe table

-- COMMAND ----------

-- MAGIC %run "../includes/Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_py_new")

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

describe extended race_results_py_new

-- COMMAND ----------

select *
from demo.race_results_py_new limit 10

-- COMMAND ----------

create table demo.race_results_sql
as 
select *
from demo.race_results_py_new where race_year =2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

describe extended demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###External Table
-- MAGIC ####Learning Objectives
-- MAGIC 1. Create External table using python
-- MAGIC 2. Create external table using sql
-- MAGIC 3. effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py_new")
-- MAGIC #location our mount

-- COMMAND ----------

desc extended demo.race_results_ext_py_new

-- COMMAND ----------

select * from demo.race_results_ext_py_new limit 2

-- COMMAND ----------

create table if not exists demo.race_results_ext_sql
(grid int,
fastest_lap int,
race_time string,
points float,
positon int,
team_name string,
driver_name string,

driver_nationality string,
driver_number int,

race_name string,
race_date timestamp,
race_year int,
circuit_location string,

created_date timestamp)
using parquet
location "abfss://presentation@formula1djulia.dfs.core.windows.net/race_results_ext_sql"


-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py_new where race_year = 2020

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

desc extended demo.race_results_ext_py_new

-- COMMAND ----------

desc extended demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### View on tables
-- MAGIC #####Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent Temp View

-- COMMAND ----------

create or replace temp view v_racec_results
as 
select * 
from demo.race_results_py_new
where race_year = 2018

-- COMMAND ----------

select * from v_racec_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as 
select * 
from demo.race_results_py_new
where race_year = 2012

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

create or replace view demo.pv_race_results
as 
select * 
from demo.race_results_py_new
where race_year = 2000

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

show tables in default