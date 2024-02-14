# Databricks notebook source
# MAGIC %sql
# MAGIC use database f1_demo

# COMMAND ----------

drivers_day1_df = spark.read.option("inferSchema", True).json("/mnt/formula1datastore/bronze/2021-03-28/drivers.json")\
.filter("driverId<=10")\
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read.option("inferSchema", True).json("/mnt/formula1datastore/bronze/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId","dob",upper("name.forename").alias('forename'),upper("name.surname").alias('surname'))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read.option("inferSchema", True).json("/mnt/formula1datastore/bronze/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
.select("driverId","dob",upper("name.forename").alias('forename'),upper("name.surname").alias('surname'))

# COMMAND ----------

drivers_day3_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR Replace TABLE f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE, 
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day1 SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_date()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate)
# MAGIC   VALUES (driverId, dob, forename, surname, current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Day2 SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC   tgt.dob = upd.dob,
# MAGIC   tgt.forename = upd.forename,
# MAGIC   tgt.surname = upd.surname,
# MAGIC   tgt.updatedDate = current_date()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate)
# MAGIC   VALUES (driverId, dob, forename, surname, current_date())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge order by driverId

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day3 Python

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datastore/demo/drivers_merge')

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
        "dob":"upd.dob",
        "forename":"upd.forename",
        "surname":"upd.surname",
        "updatedDate":"current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {   "driverId":"upd.driverId",
        "dob":"upd.dob",
        "forename":"upd.forename",
        "surname":"upd.surname",
        "createdDate":"current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge order by driverId

# COMMAND ----------


