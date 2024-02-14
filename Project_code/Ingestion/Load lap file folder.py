# Databricks notebook source
# MAGIC %md
# MAGIC ### Define Schema and apply to the df

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

# Shift+ctrl+l: make changes to selected all 
data_schema = [StructField('raceId',IntegerType(),True), 
               StructField('driverId',IntegerType(),True),
               StructField('lap',IntegerType(),True),
               StructField('position',IntegerType(),True),
               StructField('time',StringType(),True),
               StructField('milliseconds',IntegerType(),True),
               ]
laptimes_schema = StructType(fields = data_schema)

# COMMAND ----------

# Read all the files under the folder
# It will be combined to a single dtaframe
laptimes_df = spark.read.csv("/mnt/formula1datastore/bronze/lap_times",schema= laptimes_schema)
# Or you can specify the wildcard path to pick up certain files from the folder
laptimes_df = spark.read.csv("/mnt/formula1datastore/bronze/lap_times/lap_time_split*.csv",schema= laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add and Rename Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#df.withColumnRenaed(existing_col_name,new_col_name)
laptimes_df = laptimes_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('ingestion_date',current_timestamp())

display(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the file to data lake

# COMMAND ----------

laptimes_df.write.parquet("/mnt/formula1datastore/silver/lap_times",mode = "overwrite") 
