# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col, when, desc, rank

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year","driver_name","driver_nationality","team")\
.agg(sum("points").alias("total_points"),
     count(when (col("position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_standings_df.filter("race_year =2020").orderBy(desc("wins")).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

#define the window
driverRank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
# apply the window with the rank function
final_df = driver_standings_df.withColumn("rank",rank().over(driverRank))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{gold_path}/driver_standings")

# COMMAND ----------


