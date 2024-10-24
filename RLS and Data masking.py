# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}
# MAGIC
# MAGIC USE CATALOG ${DA.my_new_catalog}
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS example;
# MAGIC USE SCHEMA example

# COMMAND ----------

# MAGIC %md
# MAGIC #PERMISSIONS
# MAGIC
# MAGIC To see grants

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT ON TABLE ${DA.catalog_name}.patient_gold.heartrate_stats

# COMMAND ----------

# MAGIC %md
# MAGIC Unity Catalog employs an explicit permission model by default; no permissions are implied or inherited from containing elements. Therefore, in order to access any data objects, users will need USAGE permission on all containing elements; that is, the containing schema and catalog.
# MAGIC Now let's allow members of the account users group to query the gold view. In order to do this, we need to grant the following permissions:
# MAGIC 	1. USAGE on the catalog and schema
# MAGIC 	2. SELECT on the data object (e.g. view)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO `account users`;
# MAGIC
# MAGIC GRANT USAGE ON SCHEMA example TO `account users`;
# MAGIC
# MAGIC GRANT SELECT ON VIEW agg_heartrate to `account users`

# COMMAND ----------

# MAGIC %md
# MAGIC #FUNCTION 
# MAGIC
# MAGIC Unity Catalog is capable of managing user-defined functions within schemas as well. The code below sets up a simple function that masks all but the last two characters of a string, and then tries it out. Once again, we are the data owner so no grants are required.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION my_mask(x STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
# MAGIC ); 
# MAGIC SELECT my_mask('sensitive data') AS data
# MAGIC
# MAGIC GRANT EXECUTE ON FUNCTION my_mask to `account users`
# MAGIC
# MAGIC
# MAGIC SELECT current_catalog(), current_database(). --you can see default 
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS my_own_schema;
# MAGIC USE my_own_schema;
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC #DYNAMIC VIEWS
# MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do things like:
# MAGIC 	• partially obscure column values or redact them entirely
# MAGIC 	• omit rows based on specific criteria
# MAGIC 	
# MAGIC 	• current_user(): returns the email address of the user querying the view
# MAGIC 	• is_account_group_member(): returns TRUE if the user querying the view is a member of the specified group
# MAGIC 	
# MAGIC 	 legacy function  -  is_member()
# MAGIC 	
# MAGIC 	REDACT VALUES OR RESTRICT ROWS
# MAGIC 	Not the best practice, just an example 

# COMMAND ----------

# MAGIC %sql
# MAGIC 	CREATE OR REPLACE VIEW agg_heartrate AS
# MAGIC 	SELECT
# MAGIC 	  CASE WHEN
# MAGIC 	    is_account_group_member('account users') THEN 'REDACTED'
# MAGIC 	    ELSE mrn
# MAGIC 	  END AS mrn,
# MAGIC 	  CASE WHEN
# MAGIC 	    is_account_group_member('account users') THEN 'REDACTED'
# MAGIC 	    ELSE name
# MAGIC 	  END AS name,
# MAGIC 	  MEAN(heartrate) avg_heartrate,
# MAGIC 	  DATE_TRUNC("DD", time) date
# MAGIC 	  FROM heartrate_device
# MAGIC 	  GROUP BY mrn, name, DATE_TRUNC("DD", time)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use case when in predicate to restrict rows

# COMMAND ----------

# MAGIC %sql
# MAGIC 	CREATE OR REPLACE VIEW agg_heartrate AS
# MAGIC 	SELECT
# MAGIC 	  mrn,
# MAGIC 	  time,
# MAGIC 	  device_id,
# MAGIC 	  heartrate
# MAGIC 	FROM heartrate_device
# MAGIC 	WHERE
# MAGIC 	  CASE WHEN
# MAGIC 	    is_account_group_member('account users') THEN device_id < 30
# MAGIC 	    ELSE TRUE
# MAGIC 	  END

# COMMAND ----------

# MAGIC %md
# MAGIC 	After changes we have to Re-issue the grant.

# COMMAND ----------

GRANT SELECT ON VIEW agg_heartrate to `account users`

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA MASKING#

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC 	DROP VIEW IF EXISTS agg_heartrate;
# MAGIC 	CREATE VIEW agg_heartrate AS
# MAGIC 	SELECT
# MAGIC 	  CASE WHEN
# MAGIC 	    is_account_group_member('account users') THEN my_mask(mrn)
# MAGIC 	    ELSE mrn
# MAGIC 	  END AS mrn,
# MAGIC 	  time,
# MAGIC 	  device_id,
# MAGIC 	  heartrate
# MAGIC 	FROM heartrate_device
# MAGIC 	WHERE
# MAGIC 	  CASE WHEN
# MAGIC 	    is_account_group_member('account users') THEN device_id < 30
# MAGIC 	    ELSE TRUE
# MAGIC 	  END
# MAGIC 	
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC To revoke grants

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC 	REVOKE EXECUTE ON FUNCTION my_mask FROM `account users`
# MAGIC 	
# MAGIC 	REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM `account users`
