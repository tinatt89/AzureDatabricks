-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC circuits_df = spark.read.parquet(f"{silver_path}/circuits")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use Python to create a managed table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # you need to specify the database name 
-- MAGIC circuits_df.write.saveAsTable("demo_db.circuits_python",format="parquet")

-- COMMAND ----------

Describe extended circuits_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use sql to create a managed table

-- COMMAND ----------

use demo_db

-- COMMAND ----------

CREATE TABLE circuits_sql
AS
SELECT * FROM circuits_python

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External Table
-- MAGIC The way to create external table is quiet similar to the way to create managed table, the major difference is you have to specify the place where to store the data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df.write.option("Path",f"{gold_path}/circuits_python_external").saveAsTable("demo_db.circuits_python",format="parquet")

-- COMMAND ----------

create table demo_db.circuits_sql(
  col_name type,
  ...
)
using parquet
location '/mnt/formula1datastore/gold/circuits_sql'

-- COMMAND ----------

Insert into demo_db.circuits_sql
Select * from demo_db.circuits_python
