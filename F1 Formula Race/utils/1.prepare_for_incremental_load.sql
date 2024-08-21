-- Databricks notebook source
drop database if exists f1_processed cascade

-- COMMAND ----------

-- MAGIC %md
-- MAGIC drop all the tables in the database and database

-- COMMAND ----------

create database if not exists f1_processed

-- COMMAND ----------

Drop database if exists f1_presentation cascade

-- COMMAND ----------

create database if not exists f1_presentation