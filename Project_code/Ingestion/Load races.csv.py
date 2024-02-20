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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, TimestampType

# COMMAND ----------

data_schema = [StructField('raceId',IntegerType(),True), 
               StructField('year',IntegerType(),True),
               StructField('round',IntegerType(),True),
               StructField('circuitId',IntegerType(),True),
               StructField('name',StringType(),True),
               StructField('date',StringType(),True),
               StructField('time',StringType(),True),
               StructField('url',StringType(),True),
               ]
races_schema = StructType(fields = data_schema)

# COMMAND ----------

races_df = spark.read.csv(f"{bronze_path}/{filedate}/races.csv",header= True, schema = races_schema )

# COMMAND ----------

display(races_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Columns

# COMMAND ----------

races_df = races_df.withColumnRenamed('raceId','race_id').withColumnRenamed('year','race_year').withColumnRenamed('circuitId','circuit_id')
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,lit,concat,col

# COMMAND ----------

races_df = races_df.withColumn('ingestion_date',current_timestamp())

races_df = races_df.withColumn('race_timestamp',to_timestamp(concat(col('date'), lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select Columns

# COMMAND ----------

races_df = races_df.select('race_id','race_year','round','circuit_id','name','ingestion_date','race_timestamp')
display(races_df)

# COMMAND ----------

races_df = races_df.withColumn("data_source", lit(datasource)).withColumn("file_date", lit(filedate))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Load

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_silver.races")

# COMMAND ----------

display(spark.read.parquet(f"{silver_path}/races"))
