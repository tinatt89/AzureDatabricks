# Databricks notebook source
# MAGIC %md
# MAGIC ### Define a Schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

data_schema = [StructField('raceId',IntegerType(),False), 
               StructField('driverId',IntegerType(),True),
               StructField('stop',StringType(),True),
               StructField('lap',IntegerType(),True),
               StructField('time',StringType(),True),
               StructField('duration',StringType(),True),
               StructField('milliseconds',IntegerType(),True)]
pitstops_schema = StructType(fields = data_schema)

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).option("multiLine", True).json("/mnt/formula1datastore/bronze/pit_stops.json")
# By default, spark works with signle line json file, so you have to specify it if you are working with a multi=line json file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and Add an audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

pitstops_df = pitstops_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('Ingestion_date',current_timestamp())

# COMMAND ----------

# Write to destination
pitstops_df.write.parquet("/mnt/formula1datastore/silver/pitstops",mode = "overwrite")

# COMMAND ----------


