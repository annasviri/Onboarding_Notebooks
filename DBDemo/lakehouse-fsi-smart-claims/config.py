# Databricks notebook source
# MAGIC %md 
# MAGIC ## Configuration file
# MAGIC
# MAGIC Please change your catalog and schema here to run the demo on a different catalog.
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2Fconfig&demo_name=lakehouse-fsi-smart-claims&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-fsi-smart-claims%2Fconfig&version=1">

# COMMAND ----------

#Remember to change this in 01-Data-Ingestion/01.1-DLT-Ingest-Policy-Claims too
catalog = "main"
schema = dbName = db = "fsi_smart_claims"

volume_name = "volume_claims"
