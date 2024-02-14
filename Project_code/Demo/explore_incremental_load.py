# Databricks notebook source
# MAGIC %md
# MAGIC ### Incremental Load

# COMMAND ----------

# MAGIC %md
# MAGIC Method_1

# COMMAND ----------

for race_id_list in results_df.select("race_id").distinct().collect():
    if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

results_df.write("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC Method_2

# COMMAND ----------

sql.conf.set("aprk.sql.sources.partitionOverwriteMode","dynamic")
# set the partition overwrite mode to dynamic. What that means is when the insertInto runs, it's going to find the partitions and only replace
# those partitions with the new data received.It's not going to overwrite the entire table.

# COMMAND ----------

# Change the order of columns, make sure race_id is at the last
# Becuase Spark expects the last column in the list to be the partitioned column 
results_df = results_df.select("result_id","driver_id",...,"race_id") 

# COMMAND ----------

if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    #Incremental Load
     results_df.write("overwrite").insertInto("f1_processed.results")
else:
    #Initial Load
    results_df.write("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------


