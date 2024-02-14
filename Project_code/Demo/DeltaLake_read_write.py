# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1datastore/demo'

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/formula1datastore/bronze/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrtie data to a delta foramt

# COMMAND ----------

# MAGIC %md
# MAGIC ### create a managed table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an external table

# COMMAND ----------

# save data to the specified location
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1datastore/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC /*create an external table */
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1datastore/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data from a delta file

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1datastore/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------


