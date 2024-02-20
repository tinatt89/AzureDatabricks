# Databricks notebook source
# MAGIC %md
# MAGIC ### Passing Parameters

# COMMAND ----------

dbutils.widgets.text("Datasource","")
datasource = dbutils.widgets.get("Datasource")

dbutils.widgets.text("Filedate","")
filedate = dbutils.widgets.get("Filedate")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Invoke Configuration notebook

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema

# COMMAND ----------

# Define the Schema with ddl style(can choose either)
constructors_schema ="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
# Read in the as dataframe
constructors_df = spark.read.json(f"{bronze_path}/{filedate}/constructors.json",schema=constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

dropped_constructors_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and Add audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# Rename columns and add columns
final_constructors_df = dropped_constructors_df.withColumnRenamed('constructorId','constructor_id')\
                                               .withColumnRenamed('constructorRef', 'constructor_ref')\
                                               .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

final_constructors_df = final_constructors_df.withColumn("data_source", lit(datasource)).withColumn("file_date", lit(filedate))

# COMMAND ----------

display(final_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Load

# COMMAND ----------

final_constructors_df.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.constructors")

# COMMAND ----------


