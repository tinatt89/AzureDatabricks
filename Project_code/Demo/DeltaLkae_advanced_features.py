# Databricks notebook source
# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Style

# COMMAND ----------

# MAGIC %md
# MAGIC ### Timetravel based on version number

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 7

# COMMAND ----------

# MAGIC %md
# MAGIC ### Timetravel based on timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-01-29T23:53:19Z'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python Style

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf",'2024-01-29T23:53:19Z').load("mnt/formula1datastore/demo/drivers_merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------


