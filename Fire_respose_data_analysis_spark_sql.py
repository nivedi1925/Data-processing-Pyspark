# Databricks notebook source
# MAGIC %md
# MAGIC ### San Francisco Fire Calls Dataset analysis Using SparkSQL

# COMMAND ----------

# MAGIC %md
# MAGIC Data source: /databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv<br>
# MAGIC <br>
# MAGIC <br>
# MAGIC Goal of this notebook is to load the San Franscisco fire call respose dataset and answer the bellow questions for analysis purpose.
# MAGIC In this analysis we use spark sql.

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC Load the data file into spark table. And find the answers to below question.
# MAGIC 1. How many disctinct types of calls were made to fire department?
# MAGIC 2. What are the distict types of calls made to the fire department?
# MAGIC 3. Find out all responses or delayed times greater than 5 mins?
# MAGIC 4. What were the most common call types?
# MAGIC 5. What zip codes accounted for most common calls?
# MAGIC 6. What san franciso neighbours are in the zip codes 94102 and 94103
# MAGIC 7. What was the sum of calls, averages, min, and max of the call response times?
# MAGIC 8. How many distinct year of data are in the csv file?
# MAGIC 9. What week of the year in 2018 had the most fire calls?
# MAGIC 10. What neighborhoods in San francisco had the worst resposnse time in 2018?

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists fire_response_db; 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %md
# MAGIC Remove the spark sql table if its exist in extrenal storage HDFS. 

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/fire_response_db.db/", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists fire_response_db.fire_service_calls_tbl(
# MAGIC   CallNumber integer,
# MAGIC UnitID string,
# MAGIC IncidentNumber integer,
# MAGIC CallType string,
# MAGIC CallDate string,
# MAGIC WatchDate string,
# MAGIC  CallFinalDisposition string,
# MAGIC  AvailableDtTm string,
# MAGIC  Address string,
# MAGIC  City string,
# MAGIC  Zipcode integer,
# MAGIC  Battalion string,
# MAGIC  StationArea string,
# MAGIC  Box string,
# MAGIC  OriginalPriority string,
# MAGIC  Priority string,
# MAGIC  FinalPriority integer,
# MAGIC  ALSUnit boolean,
# MAGIC  CallTypeGroup string,
# MAGIC  NumAlarms integer,
# MAGIC  UnitType string,
# MAGIC  UnitSequenceInCallDispatch integer,
# MAGIC  FirePreventionDistrict string,
# MAGIC  SupervisorDistrict string,
# MAGIC  Neighborhood string,
# MAGIC  Location string,
# MAGIC  RowID string,
# MAGIC  Delay float
# MAGIC ) using parquet;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load the data into table
# MAGIC <br>
# MAGIC To achieve this,<br>1.first we need to read the data in the pyspark data frame,<br>
# MAGIC 2.And then save it as globalTempView.<br>
# MAGIC 3.And at the last load it into table.

# COMMAND ----------

fire_df = spark.read \
  .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.count()

# COMMAND ----------

fire_df.createGlobalTempView("fire_service_calls_view")#this will create view in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table fire_response_db.fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into  fire_response_db.fire_service_calls_tbl 
# MAGIC (select * from  global_temp.fire_service_calls_view);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire_response_db.fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fire_response_db.fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view fire_service_cal_tbl_cache;

# COMMAND ----------

# MAGIC %sql
# MAGIC  cache lazy table fire_service_cal_tbl_cache as
# MAGIC  select * from fire_response_db.fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %md 1. How many disctinct types of calls were made to fire department?

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct CallType ) from fire_response_db.fire_service_calls_tbl WHERE CallType is not null;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. What are the distict types of calls made to the fire department?

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct CallType as Distinct_call_types_list from fire_response_db.fire_service_calls_tbl;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Find out all responses or delayed times greater than 5 mins?

# COMMAND ----------

# MAGIC %sql
# MAGIC select callNumber,delay from fire_response_db.fire_service_calls_tbl where delay>5;

# COMMAND ----------

# MAGIC %md
# MAGIC 4. What were the most common call types?

# COMMAND ----------

# MAGIC %sql
# MAGIC select callType,count(callType)
# MAGIC from fire_response_db.fire_service_calls_tbl 
# MAGIC where callType is not null
# MAGIC group by CallType
# MAGIC  order by count(callType) desc limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 5. What zip codes accounted for most common calls?

# COMMAND ----------

# MAGIC %sql
# MAGIC select zipcode,callType,count(callType) as count
# MAGIC from fire_response_db.fire_service_calls_tbl
# MAGIC where callType is not null
# MAGIC group by callType,Zipcode
# MAGIC order by count desc;

# COMMAND ----------

# MAGIC %md
# MAGIC 6. What san franciso neighbours are in the zip codes 94102 and 94103

# COMMAND ----------

# MAGIC %sql
# MAGIC select Neighborhood,Zipcode 
# MAGIC from fire_response_db.fire_service_calls_tbl
# MAGIC  where zipcode in (94102,94103);

# COMMAND ----------

# MAGIC %md
# MAGIC 7. What was the sum of num alarms, averages, min, and max of the call response times?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(NumAlarms), avg(delay),min(delay),max(delay)
# MAGIC from fire_response_db.fire_service_calls_tbl;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 8. How many distinct year of data are in the csv file?

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(year(to_date(callDate,"yyyy-mm-dd")))) as year_num
# MAGIC from fire_response_db.fire_service_calls_tbl
# MAGIC order by year_num

# COMMAND ----------

# MAGIC %md
# MAGIC 9. What week of the year in 2018 had the most fire calls?

# COMMAND ----------

# MAGIC %sql
# MAGIC select weekofyear(to_date(calldate,"yyyy-mm-dd")) as week_year,count(*) as count
# MAGIC from fire_response_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate)) == 2018
# MAGIC group by week_year
# MAGIC order by count

# COMMAND ----------

# MAGIC %md
# MAGIC 10. What neighborhoods in San francisco had the worst resposnse time in 2018?

# COMMAND ----------

# MAGIC %sql
# MAGIC select Neighborhood,delay
# MAGIC from fire_response_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate,"yyyy-mm-dd"))==2018
# MAGIC order by delay desc
# MAGIC