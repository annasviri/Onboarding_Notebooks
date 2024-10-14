# Databricks notebook source
# MAGIC %pip install dbdemos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC import dbdemos
# MAGIC dbdemos.list_demos()
# MAGIC dbdemos.install('lakehouse-retail-c360')

# COMMAND ----------

import dbdemos
dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-retail-c360')

# COMMAND ----------

import dbdemos
dbdemos.install('lakehouse-fsi-smart-claims', catalog='main', schema='fsi_smart_claims')
