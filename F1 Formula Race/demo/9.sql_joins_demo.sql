-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

-- DBTITLE 1,andin
desc driver_standings

-- COMMAND ----------

create or replace temp view driver_standing_2018
as select race_year, driver_name, team_name,total_points, wins,rank from driver_standings where race_year = 2018;

-- COMMAND ----------

create or replace temp view driver_standing_2020
as select race_year, driver_name, team_name,total_points, wins,rank from driver_standings where race_year = 2020;

-- COMMAND ----------

select *
from driver_standing_2018 d_2018
join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select *
from driver_standing_2018 d_2018
left join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select *
from driver_standing_2018 d_2018
right join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select *
from driver_standing_2018 d_2018
semi join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select *
from driver_standing_2018 d_2018
anti join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name;


-- COMMAND ----------

select *
from driver_standing_2018 d_2018
cross join driver_standing_2020 d_2020
on  d_2018.driver_name = d_2020.driver_name