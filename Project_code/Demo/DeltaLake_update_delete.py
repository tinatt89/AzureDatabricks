# Databricks notebook source
# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update

# COMMAND ----------

# MAGIC %md
# MAGIC ### sql syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC Update f1_demo.results_managed
# MAGIC set points = 11 - position
# MAGIC Where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Syntax

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datastore/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete

# COMMAND ----------

# MAGIC %md
# MAGIC ### sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC Where position > 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datastore/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")


# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from f1_demo.results_managed

# COMMAND ----------


