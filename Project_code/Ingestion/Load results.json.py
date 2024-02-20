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

results_df = spark.read.json(f"{bronze_path}/{filedate}/results.json",schema= results_schema)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and Add an audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col, lit

# COMMAND ----------

results_df = results_df.withColumnRenamed('resultId','result_id').withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id').withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order').withColumnRenamed('fastestLap','fastest_lap').withColumnRenamed('fastestLapTime','fastest_lap_time').withColumnRenamed('fastestLapSpeed','fastest_lap_speed').withColumn('Ingestion_date',current_timestamp())

# COMMAND ----------

results_df = results_df.withColumn("data_source", lit(datasource)).withColumn("file_date", lit(filedate))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop columns

# COMMAND ----------

results_df = results_df.drop(col("statusId"))

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Load

# COMMAND ----------

# Write to destination
results_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_silver.results")

# COMMAND ----------

# MAGIC %md
# MAGIC The data lake with parquet format doesnot support deletion and update operation, you only have insert and load, but it can drop partition.  
# MAGIC The table is partition by race_id.

# COMMAND ----------

# MAGIC %md
# MAGIC Method_1

# COMMAND ----------

# collect can return the result as a list, but be careful with collect() method, because it writes the data to  the driver node's memory, so we need to make sure that we're only collecting small amount of data.
for race_id_list in results_df.select("race_id").distinct().collect():
    if(spark._jsparkSession.catalog().tableExists("f1_silver.results")):
        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

results_df.write("append").partitionBy('race_id').format("parquet").saveAsTable("f1_silver.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select racec_id, count(1)
# MAGIC from f1_silver.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method_2

# COMMAND ----------

overwrite_partition(results_df,'f1_silver','results','race_id')
