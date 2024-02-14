# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{silver_path}/circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC use database demo_db

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Temp View

# COMMAND ----------

# MAGIC %md
# MAGIC Python Way to Create

# COMMAND ----------

circuits_df.createOrReplaceTempView("local_tempv_python")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Way to Create

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace TEMP VIEW local_tempv_sql
# MAGIC As 
# MAGIC Select * 
# MAGIC From local_tempv_python

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global temp view

# COMMAND ----------

# MAGIC %md
# MAGIC Python Way to Create

# COMMAND ----------

circuits_df.createOrReplaceGlobalTempView("global_tempv_python")
# Spark will register that view against a database called global_temp.
# So you have to specify the name of database, otherwise it will use the default database and throw an error

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Way to Create

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace Global TEMP VIEW global_tempv_sql
# MAGIC As 
# MAGIC Select * 
# MAGIC From local_tempv_python

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permenant View

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or Replace View pv_sql
# MAGIC AS
# MAGIC Select * 
# MAGIC From circuits_python

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo_db

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Data with sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from local_tempv_python

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from global_temp.global_tempv_python
# MAGIC /*You have to specify global_temp as the database to access the global view*/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query with Python

# COMMAND ----------

display(spark.sql("select * from global_temp.global_temp_v"))
# Benefits
# it returns a dataframe, so you can store the results for later usage
# you can use variables inside the statement
country_name = 'Australia'
results = spark.sql(f"Select * from temp_v where country ='{country_name}'")

# COMMAND ----------


