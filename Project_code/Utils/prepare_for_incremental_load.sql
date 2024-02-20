-- Databricks notebook source
Drop database if exists f1_silver cascade

-- COMMAND ----------

Create database if not exists f1_silver
Location "/mnt/formula1datastore/silver"

-- COMMAND ----------

Drop database if exists f1_gold cascade

-- COMMAND ----------

Create database if not exists f1_gold
Location "/mnt/formula1datastore/gold"

-- COMMAND ----------


