# Databricks notebook source
# MAGIC %md
# MAGIC df.join(other, on=None, how=None)  
# MAGIC other: DataFrame  
# MAGIC on: str, list or Column  
# MAGIC how: str, default inner

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# we should rename the columns with the same name before joining
circuits_df = spark.read.parquet(f"{silver_path}/circuits")\
.withColumnRenamed("name", "circuit_name")
races_df = spark.read.parquet(f"{silver_path}/races").filter("race_year = 2019")\
.withColumnRenamed("name", "race_name")

# COMMAND ----------

# Use select method to select the columns we are intereted in.
circuits_races_df= circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id)\
.select(circuits_df.circuit_name, circuits_df.location,circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(circuits_races_df)
