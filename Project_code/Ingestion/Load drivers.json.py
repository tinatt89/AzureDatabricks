# Databricks notebook source
# MAGIC %md
# MAGIC ### Define a Schema
# MAGIC a schema inside a schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields =[StructField('forename',StringType(),True),
                                  StructField('surname',StringType(),True)])

# COMMAND ----------

data_schema = [StructField('driverId',IntegerType(),False), 
               StructField('driverRef',StringType(),True),
               StructField('number',IntegerType(),True),
               StructField('code',StringType(),True),
               StructField('name',name_schema,True),
               StructField('dob',DateType(),True),
               StructField('nationality',StringType(),True),
               StructField('url',StringType(),True),
               ]
drivers_schema = StructType(fields = data_schema)

# COMMAND ----------

drivers_df = spark.read.json("/mnt/formula1datastore/bronze/drivers.json",schema= drivers_schema)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns and Add an audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('driverId','driver_id').withColumnRenamed('driverRef','driver_ref').withColumn('ingestion_date',current_timestamp()).withColumn('name',concat(col("name.forename"),lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop columns

# COMMAND ----------

drivers_df = drivers_df.drop(col("url"))

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# Write to destination
drivers_df.write.parquet("/mnt/formula1datastore/silver/drivers",mode = "overwrite")
