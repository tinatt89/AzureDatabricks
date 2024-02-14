# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md
# MAGIC Usually, we would go with ADF to automate the execution of notebooks. Here we are using dbutils.notebook module for demonstration.

# COMMAND ----------

dbutils.notebook.run("../Ingestion/Load circuits.csv",0,{"datasource":"Ergast API"})

# COMMAND ----------

# MAGIC %md
# MAGIC if that has failed/successed, then you've got nothing to tell us that actually there is a failure/success.
# MAGIC So what you can do is you can add the dbutils.notebook. exit command to add an exit status to the other notebook. so you can capture the exit code and then execute the following notebooks, only based on the condition there.

# COMMAND ----------

# MAGIC %md
# MAGIC To run notebooks in parallel.  
# MAGIC Generally these kind of things are not done in Databricks, because of its limited capability within the scheduler itself.
# MAGIC You'll have to write Python code like this in order to achieve concurrent execution    
# MAGIC So instead of this in a production scenario, you would go to Azure Data Factory, because that gives you a rich set of capabilities like you can execute things in parallel  
