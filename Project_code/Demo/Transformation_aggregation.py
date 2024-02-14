# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_path}/race_results")

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple Aggregation Functions

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
.withColumnRenamed("sum(points)","total_point")\
.withColumnRenamed("count(DISTINCT race_name)","number_of_races")\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group By

# COMMAND ----------

demo_df.groupBy("driver_name").sum("points").show()
# After applying groupBy, the dataframe => grouped data, where you can perform any kinds of aggragtion function to it. 
# But once you applied the aggregation function, it becomes a dataframe again, so it means you can only apply one aggregation  fucntion

# COMMAND ----------

#To apply multiple aagregation function at one time, you can use agg() function
demo_df.groupBy("driver_name").agg(sum("points").alias("total_point"), countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Function

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

# create a dataframe suitable for window function case
grouped_df = demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_point"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

#define the window
driverRank = Window.partitionBy("race_year").orderBy(desc("total_point"))
# apply the window with the rank function
grouped_df.withColumn("rank",rank().over(driverRank)).show()

# COMMAND ----------


