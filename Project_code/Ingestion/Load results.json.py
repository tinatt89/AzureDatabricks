# Databricks notebook source
# MAGIC %md
# MAGIC ### Define a Schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

data_schema = [StructField('resultId',IntegerType(),False), 
               StructField('raceId',IntegerType(),True),
               StructField('driverId',IntegerType(),True),
               StructField('constructorId',IntegerType(),True),
               StructField('number',IntegerType(),True),
               StructField('grid',IntegerType(),True),
               StructField('position',IntegerType(),True),
               StructField('positionText',StringType(),True),
               StructField('positionOrder',IntegerType(),True),
               StructField('points',FloatType(),True),
               StructField('laps',IntegerType(),True),
               StructField('time',StringType(),True),
               StructField('milliseconds',IntegerType(),True),
               StructField('fastestLap',IntegerType(),True),
               StructField('rank',IntegerType(),True),
               StructField('fastestLapTime',StringType(),True),
               StructField('fastestLapSpeed',FloatType(),True),
               StructField('statusId',StringType(),True),
               ]
results_schema = StructType(fields = data_schema)

# COMMAND ----------

results_df = spark.read.json("/mnt/formula1datastore/bronze/results.json",schema= results_schema)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and Add an audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

results_df = results_df.withColumnRenamed('resultId','result_id').withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id').withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order').withColumnRenamed('fastestLap','fastest_lap').withColumnRenamed('fastestLapTime','fastest_lap_time').withColumnRenamed('fastestLapSpeed','fastest_lap_speed').withColumn('Ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop columns

# COMMAND ----------

results_df = results_df.drop(col("statusId"))

# COMMAND ----------

display(results_df)

# COMMAND ----------

# Write to destination
results_df.write.parquet("/mnt/formula1datastore/silver/results",mode = "overwrite")

# COMMAND ----------


