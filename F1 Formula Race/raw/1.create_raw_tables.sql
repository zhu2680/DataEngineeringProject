-- Databricks notebook source
-- MAGIC %run ../includes/Configuration

-- COMMAND ----------

create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Circuits Table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitId INT,
circuitRef STRING,
name String,
location string,
country string,
lat double,
lng double,
 alt integer,
 url STRING)
 using csv
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/circuits.csv",header true)

-- COMMAND ----------

describe extended f1_raw.circuits

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Races Table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId integer,

year int,
round int,
circuitId int,

name string,

date date,
time string,
url string
 )
 using csv
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Json File
-- MAGIC ######Create Construction single line JSON File

-- COMMAND ----------


drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId INT, 
constructorRef STRING,
 name STRING, 
 nationality STRING,
  url STRING
 )
 using json
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/constructors.json",header true)

-- COMMAND ----------


select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Driver table
-- MAGIC #####complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId integer,
driverRef string,
number int,
code string,
name struct<forename:string, surname:string>,
dob date,
nationality string,
url string
 )
 using json
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId integer,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points int,
laps int,
time string,
milliseconds int,
fastestLap string,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string
 )
 using json
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create pitsstops table
-- MAGIC ##### multiline json

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId int,
driverId int,
stop int,
lap int,
time string,
duration string,
milliseconds int
 )
 using json
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create multi file table Laptimes and Qualifying files
-- MAGIC __

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId int,
driverId integer,
lap string,
position int,
time string,
milliseconds int
 )
 using csv
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2  string,
q3 string
 )
 using json
 options(path "abfss://raw@formula1djulia.dfs.core.windows.net/qualifying", multiLine True)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

