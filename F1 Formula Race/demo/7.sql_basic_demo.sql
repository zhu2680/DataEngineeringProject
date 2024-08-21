-- Databricks notebook source
show databases

-- COMMAND ----------

use f1_processed;
show tables

-- COMMAND ----------

-- DBTITLE 1,ables
select * from drivers limit 10;


-- COMMAND ----------

desc drivers;


-- COMMAND ----------

select * from drivers where nationality = 'Australian';