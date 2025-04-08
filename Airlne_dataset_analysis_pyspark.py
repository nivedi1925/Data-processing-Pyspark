# Databricks notebook source
# MAGIC %md
# MAGIC List of tal databricks public datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC List the Airline dataset

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/airlines/

# COMMAND ----------

# MAGIC %md
# MAGIC Since comunity cluster is not caable of handling entire dataset we use only one file from airlines dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC First lets find the format of the data file.

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/airlines/part-00000

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can read the files as csv.And header row exist.

# COMMAND ----------

airline_df = spark.read\
  .format("csv")\
  .option("inferSchema","true")\
    .option("header","true")\
      .load("/databricks-datasets/airlines/part-00000")

# COMMAND ----------

display(airline_df)

# COMMAND ----------

from pyspark.sql.functions import *
new_df = airline_df.select("Origin","Dest","Distance",expr("to_date(concat(Year,Month,DayofMonth),'yyyy-MM-dd') as FlightDate")).show()

# COMMAND ----------

from pyspark.sql.functions import *
new_df = airline_df.select("Origin","Dest","Distance",to_date(concat("Year","Month","DayofMonth"),"yyyy-MM-dd").alias("FlightDate"))\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Quick methods to create dataframes

# COMMAND ----------

data_list = [
  ("Niranjan",28,"Bagalore"),
  ("Person X",23,"Delhi")
]

# COMMAND ----------

new_df = spark.createDataFrame(data_list).toDF("Name","Age","Place")

# COMMAND ----------

display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding new column unique id

# COMMAND ----------

new_df = new_df.withColumn("id",monotonically_increasing_id())
display(new_df)

# COMMAND ----------

