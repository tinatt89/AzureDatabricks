# Databricks notebook source
# circuits_df = spark.read.csv("/mnt/formula1datastore/bronze/circuits.csv",header= True, inferSchema= True)
# header: indicate whether the data source has a header
# inferSchema: indicate whether to go through the data to define its schema, not efficient, rarely used for large datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passing Parameters

# COMMAND ----------

# Define a input widget
dbutils.widgets.text("Datasource","")
# Retrieves value from an input widget
datasource = dbutils.widgets.get("Datasource")
datasource

# COMMAND ----------

# Define a input widget
dbutils.widgets.text("Filedate","")
# Retrieves value from an input widget
filedate = dbutils.widgets.get("Filedate")
filedate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Invoke Configuration notebook
# MAGIC By using the %run command, you can run a notebook inside the current notebook, and have access to all the variables/functions defined in that notebook. When you are doing a %run, you need to have that in its own cell

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema and apply to the df

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

# Shift+ctrl+l: make changes to selected all 
data_schema = [StructField('circuitId',IntegerType(),False), # key attribute
               StructField('circuitRef',StringType(),True),
               StructField('name',StringType(),True),
               StructField('location',StringType(),True),
               StructField('country',StringType(),True),
               StructField('lat',DoubleType(),True),
               StructField('lng',DoubleType(),True),
               StructField('alt',IntegerType(),True),
               StructField('url',StringType(),True),
               ]
circuits_schema = StructType(fields = data_schema)

# COMMAND ----------

circuits_df = spark.read.csv(f"{bronze_path}/{filedate}/circuits.csv",header= True, schema= circuits_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Columns
# MAGIC df.withColumnRenaed(existing_col_name,new_col_name)

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed('circuitId','circuit_id').withColumnRenamed('circuitRef','circuit_ref').withColumnRenamed('lat','latitude').withColumnRenamed('lng','longitude').withColumnRenamed('alt','altitude')

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Columns
# MAGIC WithColumn(col_name,col)  
# MAGIC Returns a new DataFrame by adding a column or replacing the existing column that has the same name.  
# MAGIC Col has to be an expression

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# Add a new column
# circuits_df = circuits_df.withColumn('ingestion_date',current_timestamp())
# current_timestamp: Returns the current timestamp at the start of query evaluation as a TimestampType column
# display(circuits_df.withColumn('env',lit('Production')))
# lit: Creates a Column of literal value

#Calling a function
circuits_df = add_ingestion_date(circuits_df)

# COMMAND ----------

circuits_df = circuits_df.withColumn("data_source", lit(datasource)).withColumn("file_date", lit(filedate))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Load

# COMMAND ----------

circuits_df.write.mode("overwrite").format("parquet").saveAsTable("f1_silver.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{silver_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit('Success')
