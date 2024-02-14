# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

# Define a input widget
dbutils.widgets.text("Datasource","")


# COMMAND ----------

# Retrieves value from an input widget
datasource = dbutils.widgets.get("Datasource")
datasource

# COMMAND ----------


