# Databricks notebook source
# DBTITLE 1,Direct Access using Service Principal
spark.conf.set("fs.azure.account.auth.type.aminebenappstorageacct.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.aminebenappstorageacct.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.aminebenappstorageacct.dfs.core.windows.net", "92779d65-3641-4588-9a5d-1d1a69e162ed")
spark.conf.set("fs.azure.account.oauth2.client.secret.aminebenappstorageacct.dfs.core.windows.net", "Ew/jxd1/3MiWztuRQ@zFmB-h8ND0U14E")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.aminebenappstorageacct.dfs.core.windows.net", "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token")

# COMMAND ----------

df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
dbutils.fs.ls("abfss://third-container@aminebenappstorageacct.dfs.core.windows.net/")

# COMMAND ----------

import time
 
# Wait for 5 seconds
time.sleep(60)

# COMMAND ----------

df.write.mode("overwrite").json("abfss://third-container@aminebenappstorageacct.dfs.core.windows.net/iot_devices-3.json")

# COMMAND ----------

import time
 
# Wait for 5 seconds
time.sleep(600)

# COMMAND ----------

# DBTITLE 1,Direct Access using Storage Account Access key
spark.conf.set("fs.azure.account.key.aminebenappstorageacct.dfs.core.windows.net", "auIvKaD4r7IK+ADqGoFBbya4HFhaYqUhJautRDPW+FmXsm5llogMdEdeTLAG4tkdNhtjVdfS62GnDlBSGjUNcQ==")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://fourth-container@aminebenappstorageacct.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
dbutils.fs.ls("abfss://fourth-container@aminebenappstorageacct.dfs.core.windows.net/")
df.write.json("abfss://fourth-container@aminebenappstorageacct.dfs.core.windows.net/iot_devices.json")

# COMMAND ----------

# DBTITLE 1,Mount using Storage Key
dbutils.fs.mount(
  source = "wasbs://first-container@aminebenappstorageacct.blob.core.windows.net",
  mount_point = "/mnt/amineben-app-20",
  extra_configs = {"fs.azure.account.key.aminebenappstorageacct.blob.core.windows.net":"auIvKaD4r7IK+ADqGoFBbya4HFhaYqUhJautRDPW+FmXsm5llogMdEdeTLAG4tkdNhtjVdfS62GnDlBSGjUNcQ=="})

# COMMAND ----------

# DBTITLE 1,Mount using Service Principal credential
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "92779d65-3641-4588-9a5d-1d1a69e162ed",
           "fs.azure.account.oauth2.client.secret": "Ew/jxd1/3MiWztuRQ@zFmB-h8ND0U14E",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://first-container@aminebenautoloadersa.dfs.core.windows.net/data",
  mount_point = "/mnt/amineben-app-15",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/amineben-app-20

# COMMAND ----------

dbutils.fs.unmount("/mnt/<mount-name>")
