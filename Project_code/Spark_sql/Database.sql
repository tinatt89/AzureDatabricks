-- Databricks notebook source
/*Create database*/
Create database if not exists demo_db

-- COMMAND ----------

/*Show all databases*/
show databases

-- COMMAND ----------

DESCRIBE database demo_db

-- COMMAND ----------

/*Show current database*/
Select current_database()

-- COMMAND ----------

/*Switch database*/
use demo_db

-- COMMAND ----------

Select current_database()

-- COMMAND ----------

/*Show tables in the current database*/
show tables

-- COMMAND ----------

/*Show tables in the specified database*/
show tables in default

-- COMMAND ----------


