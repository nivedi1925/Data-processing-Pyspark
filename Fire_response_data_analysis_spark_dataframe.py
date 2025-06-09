# Databricks notebook source
# MAGIC %md
# MAGIC ### San Francisco Fire Calls Dataset analysis Using SparkSQL

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Data source: /databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv<br>
# MAGIC <br>
# MAGIC <br>
# MAGIC Goal of this notebook is to load the San Franscisco fire call respose dataset and answer the bellow questions for analysis purpose.
# MAGIC In this analysis we use spark dataframe api.

# COMMAND ----------

# MAGIC %md
# MAGIC Load the data file into spark data frame.
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

from pyspark.sql.functions import *
#spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY") # added to resolve the error

# COMMAND ----------

file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

# COMMAND ----------

#Load the data file as dataframe 
# read is object which is again attribute of spark session
fire_df = spark.read \
          .format("csv") \
          .option("header","true") \
          .option("inferSchema","true") \
          .load(file)

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.show(4)

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

fire_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Column names are not standardized. So first need to be standardized.

# COMMAND ----------

new_df = fire_df.withColumnRenamed("Call Number","callNumber")\
.withColumnRenamed("Unit ID","UnitID")\
.withColumnRenamed("Incident Number","IncidentNumber")\
.withColumnRenamed("call date","callDate")\
.withColumnRenamed("Watch Date","WatchDate")\
.withColumnRenamed("Call Final Disposition","CallFinalDisposition")\
.withColumnRenamed("Available DtTm","AvailableDtTm")\
.withColumnRenamed("Zipcode of Incident","ZipcodeofIncident")\
.withColumnRenamed("Station Area","StationArea")\
.withColumnRenamed("Final Priority","FinalPriority")\
.withColumnRenamed("ALS Unit","ALSUnit")\
.withColumnRenamed("Call Type Group","CallTypeGroup")\
.withColumnRenamed("Unit sequence in call dispatch","Unitsequenceincalldispatch")\
.withColumnRenamed("Fire Prevention District","FirePreventionDistrict")\
.withColumnRenamed("Supervisor District","SupervisorDistrict")


# COMMAND ----------

display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date and time stamp files are still in string format.

# COMMAND ----------

new_df.printSchema()

# COMMAND ----------

fire_df = new_df.withColumn("callDate",to_date("callDate","MM/dd/yyyy"))\
        .withColumn("watchDate",to_date("callDate","MM/DD/YYYY"))
fire_df = fire_df.withColumn("AvailableDtTm",to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Round of delay column to 2 digits after the decimal

# COMMAND ----------


fire_df = fire_df.withColumn("Delay",round("Delay",2))

# COMMAND ----------

display(fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Let us cache the dataframe.Its utility method.

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now lets solve question which have been asked.

# COMMAND ----------

# MAGIC %md
# MAGIC Two methods can be used in here.<br>
# MAGIC 1. conver df into view and use sql
# MAGIC 2. dataframe transformations

# COMMAND ----------

# MAGIC %md
# MAGIC 1. How many disctinct types of calls were made to fire department?

# COMMAND ----------

q1_df = fire_df.where("callType is not null")\
    .select("callType")\
        .distinct()
print(q1_df.count())
        

# COMMAND ----------

# MAGIC %md
# MAGIC 2. What are the distict types of calls made to the fire department?

# COMMAND ----------

fire_df.where("callType is not null")\
    .select("callType")\
        .distinct()

# COMMAND ----------

display(fire_df.where("callType is not null")\
    .select("callType")\
        .distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Find out all responses or delayed times greater than 5 mins?

# COMMAND ----------

q3_df = fire_df.where("Delay>5")\
    .select("CallNumber","Delay")

q3_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC 4. What were the most common call types?

# COMMAND ----------

fire_df.where("CallType is not null")\
    .select("CallType")\
        .groupBy("callType")\
            .count()\
            .orderBy("count",ascending=False)\
                .show()


# COMMAND ----------

# MAGIC %md
# MAGIC 5. What zip codes accounted for most common calls?

# COMMAND ----------

fire_df.where("CallType is not null")\
    .select("ZipcodeofIncident","callType")\
        .groupBy("CallType")\
            .count()\
            .orderBy(["CallType","ZipcodeofIncident"],ascending=False)\
                .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. What san franciso neighbours are in the zip codes 94102 and 94103

# COMMAND ----------

fire_df.where("ZipcodeofIncident == 94102 or ZipcodeofIncident == 94103")\
    .select("Neighborhood","ZipcodeofIncident")\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. What was the sum of calls, averages, min, and max of the call response times?

# COMMAND ----------

fire_df.select(sum("NumAlarms"),min("delay"),max("delay"),avg("delay"))\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8. How many distinct year of data are in the csv file?

# COMMAND ----------

fire_df.select(expr("year(callDate)"))\
    .distinct()\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. What week of the year in 2018 had the most fire calls?

# COMMAND ----------

fire_df.filter(year("callDate") == 2018)\
    .select(expr("weekofyear(CallDate)"))\
        .groupBy(expr("weekofyear(callDate)"))\
        .count()\
        .orderBy(expr("weekofyear(callDate)")),ascending=False)
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC fire_ts_df.filter(year('IncidentDate') == 2018).groupBy(weekofyear('IncidentDate')).count().orderBy('count', ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 10. What neighborhoods in San francisco had the worst resposnse time in 2018?

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

fire_df.where()

# COMMAND ----------

while(True):
    pass

# COMMAND ----------

