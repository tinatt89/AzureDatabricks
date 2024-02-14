# Databricks notebook source
# MAGIC %md
# MAGIC df.where(condition)/filter(condition)

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{silver_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Following sql syntax

# COMMAND ----------

# single condition
races_df_filtered = races_df.filter("race_year = 1950")
# multiple conditions
races_df_filtered = races_df.filter("race_year = 1950 and round <= 5")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Following python syntax

# COMMAND ----------

# single condition
races_df_filtered = races_df.filter(races_df["race_year"] == 1950)
# multiple conditions
races_df_filtered = races_df.filter((races_df["race_year"] == 1950) & (races_df["round"] <= 5))
