-- Databricks notebook source
use f1_processed;


-- COMMAND ----------

select *, concat(driver_ref, '-',code) as new_driver_ref from drivers

-- COMMAND ----------

select *,split(name," ") from drivers

-- COMMAND ----------

select *, date_format(dob,'dd-MM-yyyy') from drivers

-- COMMAND ----------

select count(*) from drivers

-- COMMAND ----------

select max(dob) from drivers

-- COMMAND ----------

select nationality,count(*) from drivers group by nationality order by nationality

-- COMMAND ----------

select  nationality, name, dob, rank() over(partition by nationality order by dob desc) as age_rank
from drivers order by nationality, age_rank