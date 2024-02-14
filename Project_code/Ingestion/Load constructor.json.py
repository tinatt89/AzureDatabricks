# Databricks notebook source
display(spark.read.json("/mnt/formula1datastore/bronze/constructors.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema

# COMMAND ----------

# Define the Schema with ddl style(can choose either)
constructors_schema ="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
# Read in the as dataframe
constructors_df = spark.read.json("/mnt/formula1datastore/bronze/constructors.json",schema=constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unwanted columns

# COMMAND ----------

dropped_constructors_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and Add audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Rename columns and add columns
final_constructors_df = dropped_constructors_df.withColumnRenamed('constructorId','constructor_id')\
                                               .withColumnRenamed('constructorRef', 'constructor_ref')\
                                               .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

display(final_constructors_df)

# COMMAND ----------

final_constructors_df.write.parquet("/mnt/formula1datastore/silver/constructors",mode = "overwrite")
