# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos
dbdemos.install('uc-02-external-location')

# COMMAND ----------

import dbdemos
dbdemos.install('uc-05-upgrade')

# COMMAND ----------

import dbdemos
dbdemos.install('uc-04-system-tables', catalog='main', schema='billing_forecast')
