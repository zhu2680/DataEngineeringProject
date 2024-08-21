-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ####Create Managed table 

-- COMMAND ----------

create database if not exists f1_processed

-- COMMAND ----------

desc database f1_processed

-- COMMAND ----------

describe extended f1_processed.circuits