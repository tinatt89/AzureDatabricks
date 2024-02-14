# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    # client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    # tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    # client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-secret')

    # Service Principle Overview
    #client_id = "246e03e1-a83e-4a81-bc5e-e2854c04d93a"
    client_id = dbutils.secrets.get(scope='formula1-scope',key='application-client-id')
    #tenant_id = "0b1fc323-ad7c-473c-a376-f4377d4bd894"
    tenant_id = dbutils.secrets.get(scope='formula1-scope',key='application-tenant-id')
    # Generate a secret, and copy the value
    #client_secret = "DGy8Q~ce7MwWBx9DEkeJO6rSsd3P~P_lFzl3MaeE"
    client_secret = dbutils.secrets.get(scope='formula1-scope',key='application-secret-value')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
# Now we have established the connection between Azure databricks and ADLS
# The next step is to mount the ADLS to the DBFS in Azure databricks

    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    # show all the data mounted in the work space and thier storage location
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1datastore', 'demo')

# COMMAND ----------

mount_adls('formula1datastore', 'bronze')

# COMMAND ----------

mount_adls('formula1datastore', 'silver')

# COMMAND ----------

mount_adls('formula1datastore', 'gold')

# COMMAND ----------

# Show all the mounts in DBFS
display(dbutils.fs.mounts())

# COMMAND ----------

# listing files and directories in the Databricks File System (DBFS),  including details such as file names, sizes, and modification dates. 
display(dbutils.fs.ls("abfss://bronze@formula1datastore.dfs.core.windows.net"))
