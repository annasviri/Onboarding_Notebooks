# Databricks notebook source
# DBTITLE 1,Access Key is stored in AKV backed scope
spark.conf.set("fs.azure.account.key.aminebenstcs.dfs.core.windows.net", dbutils.secrets.get(scope = "amineben-kv-cs", key = "bootcamp-sa-access-key"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://bootcamp@aminebenstcs.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

dbutils.fs.ls("abfss://bootcamp@aminebenstcs.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,All information stored in a AKV 
# MAGIC %md
# MAGIC Secure method for passing keys and account info from Azure Key Vault.
# MAGIC - Key vault stores:
# MAGIC   - account name
# MAGIC   - container
# MAGIC   - key
# MAGIC   
# MAGIC <p>Key Vault secrets are stored in JSON format:
# MAGIC <pre><code>
# MAGIC   {
# MAGIC     key : -your storage key-
# MAGIC     account : -storage account name-
# MAGIC     container : -storage container / root folder-
# MAGIC   }
# MAGIC </code></pre>

# COMMAND ----------

import json

kvsecret = json.loads(dbutils.secrets.get(scope = "amineben-kv-cs", key = "amimeben-sa-all-in-one"))

spark.conf.set("fs.azure.account.key."+kvsecret["account"]+".dfs.core.windows.net", kvsecret["key"])
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls('abfss://'+kvsecret["container"]+'@'+kvsecret["account"]+'.dfs.core.windows.net/')
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
storageurl  = 'abfss://'+ kvsecret["container"] +'@'+ kvsecret["account"] +'.dfs.core.windows.net/'
print("Storage URL: storageurl=" + storageurl + " var available")

# COMMAND ----------

kvsecret

# COMMAND ----------


