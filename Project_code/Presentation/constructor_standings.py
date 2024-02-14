# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,count,col, when, desc, rank

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy("race_year","team")\
.agg(sum("points").alias("total_points"),
     count(when (col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

#define the window
constructorRank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
# apply the window with the rank function
final_df = constructor_standings_df.withColumn("rank",rank().over(constructorRank))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{gold_path}/constructor_standings")

# COMMAND ----------


