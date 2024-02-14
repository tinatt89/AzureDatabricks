# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{silver_path}/drivers")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("nationality", "driver_nationality")

constructors_df = spark.read.parquet(f"{silver_path}/constructors")\
.withColumnRenamed("name", "team")


races_df = spark.read.parquet(f"{silver_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")
circuits_df = spark.read.parquet(f"{silver_path}/circuits")\
.withColumnRenamed("name", "circuit_name")


results_df = spark.read.parquet(f"{silver_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

results_df = spark.read.parquet(f"{silver_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

circuits_races_df= circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id)

# COMMAND ----------

results_df = results_df.join(circuits_races_df,results_df.race_id == circuits_races_df.race_id)\
.join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

final_df = results_df.select(races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.location, drivers_df.driver_name,drivers_df.driver_number,drivers_df.driver_nationality, constructors_df.team,results_df.grid,results_df.fastest_lap, results_df.race_time,results_df.points,results_df.position)\
.withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name ='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{gold_path}/race_results")

# COMMAND ----------


