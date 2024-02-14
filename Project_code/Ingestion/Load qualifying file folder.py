# Databricks notebook source
# MAGIC %md
# MAGIC ### Define Schema and apply to the df

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

# Shift+ctrl+l: make changes to selected all 
data_schema = [StructField('constructorId',IntegerType(),True), 
               StructField('driverId',IntegerType(),True),
               StructField('number',IntegerType(),True),
               StructField('position',IntegerType(),True),
               StructField('q1',StringType(),True),
               StructField('q2',StringType(),True),
               StructField('q3',StringType(),True),
               StructField('qualifyId',IntegerType(),True),
               StructField('raceId',IntegerType(),True),
               ]
qualifying_schema = StructType(fields = data_schema)

# COMMAND ----------

# Read all the files under the folder
# It will be combined to a single dtaframe
qualifying_df = spark.read.json("/mnt/formula1datastore/bronze/qualifying",schema= qualifying_schema,multiLine= True)
# Or you can specify the wildcard path to pick up certain files from the folder
#laptimes_df = spark.read.csv("/mnt/formula1datastore/bronze/lap_times/lap_time_split*.csv",schema= laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add and Rename Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#df.withColumnRenaed(existing_col_name,new_col_name)
qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('raceId','race_id').withColumnRenamed('constructorId','constructor_id').withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the file to data lake

# COMMAND ----------

qualifying_df.write.parquet("/mnt/formula1datastore/silver/qualifying",mode = "overwrite") 

# COMMAND ----------


