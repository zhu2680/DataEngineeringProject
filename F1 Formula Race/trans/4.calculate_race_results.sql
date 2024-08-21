-- Databricks notebook source
show databases

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables in f1_processed

-- COMMAND ----------

drop table if exists f1_presentation.calculated_race_results

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
select races.race_year,
constructors.name as team_name,
drivers.name as driver_name, 
position,
points,
11-results.position as calculated_points

from constructors
join results on constructors.constructor_id = results.constructor_id

join races on results.race_id = races.race_id
join circuits on circuits.circuit_id = races.circuit_id
join drivers on results.driver_id = drivers.driver_id
where results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results