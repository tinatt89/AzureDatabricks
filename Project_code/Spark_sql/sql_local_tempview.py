# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframe using SQL

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

gold_path

# COMMAND ----------

circuits_df = spark.read.parquet(f"{silver_path}/circuits")

# COMMAND ----------

# Create temporary view on a dataframe


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from temp_v

# COMMAND ----------

results = spark.sql("Select * from temp_v")


# COMMAND ----------

display(results)

# COMMAND ----------

circuits_df.createOrReplaceTempView("temp_v")

# COMMAND ----------


