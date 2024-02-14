# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_columns(input_df,partition_col):
    column_list = []
    for column_name in input_df.schemas.names:
        if column_name != partition_col:
            column_list.append(column_name)
    column_list.append(partition_col)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df,db_name,table_name,partition_column):
    output_df = re_arrange_partition_columns(input_df,partition_column)
    sql.conf.set("aprk.sql.sources.partitionOverwriteMode","dynamic")
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write("overwrite").partitionBy(f'{partition_column}').format("parquet").saveAsTable(f"{db_name}.{table_name}")
