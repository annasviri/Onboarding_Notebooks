# Databricks notebook source
# MAGIC %md
# MAGIC Databricks SQL Hands on Lab
# MAGIC
# MAGIC
# MAGIC Working with TPC-H Data in Databricks SQL (DBSQL)
# MAGIC
# MAGIC Throughout this guide, you will be working with a sample dataset from the TPC-H Benchmark.  You'll have access to tables that describe customers, orders, and line item information about the orders. You will use the medallion architecture to improve the structure and quality of the data as it flows through each layer (from bronze →silver → gold layer tables). You will then create a dashboard to give us a summary of the data and set up an alert to notify us if the number of customers goes over a certain threshold. Finally, you will put all of your queries, your alert, and your dashboard into a workflow to orchestrate as a pipeline.  
# MAGIC
# MAGIC Prerequisites	2
# MAGIC Navigating Databricks SQL	3
# MAGIC STEP 1: Navigate to Databricks SQL	3
# MAGIC STEP 2: Go to the SQL persona.	4
# MAGIC Set up SQL Warehouses	4
# MAGIC STEP 1: Navigate to the SQL warehouse page	5
# MAGIC STEP 2: Create a new warehouse	5
# MAGIC STEP 3: Configure the warehouse	5
# MAGIC Explore the data	6
# MAGIC STEP 1: Navigate to the Data Explorer	6
# MAGIC STEP 2: Create a catalog	7
# MAGIC STEP 3: Create a Schema	7
# MAGIC Write SQL Queries	8
# MAGIC STEP 1:Navigate to the SQL Editor	9
# MAGIC Create Bronze Tables	9
# MAGIC STEP 1: Use the COPY INTO command to ingest your first table	9
# MAGIC STEP 2: Ingest remaining table	11
# MAGIC STEP 3: View the new tables	14
# MAGIC STEP 4: Explore the tables	15
# MAGIC Create Silver Tables	16
# MAGIC STEP 1: Create silver tables using CTAS statements	16
# MAGIC Create Visualizations	18
# MAGIC STEP 1: Write a SELECT statement to get the count of all orders	19
# MAGIC STEP 2: Create the visualization	19
# MAGIC STEP 3: Make another visualization from the orders table	21
# MAGIC Create Gold Tables	23
# MAGIC STEP 1: Make gold table for order status	25
# MAGIC STEP 2: Make visualization for the order status table	26
# MAGIC Create a Dashboard	27
# MAGIC STEP 1: Make a new dashboard	28
# MAGIC STEP 2: Add visualizations to your dashboard	29
# MAGIC STEP 3: Add remaining visualization	33
# MAGIC Create Alerts	34
# MAGIC STEP 1: Create an alert	34
# MAGIC Create a workflow	37
# MAGIC STEP 1: Save all queries	37
# MAGIC STEP 2: Navigate to workflows	37
# MAGIC STEP 3: Create workflow	38
# MAGIC STEP 4: Add your first task	39
# MAGIC STEP 5: Add remaining bronze table queries as tasks	41
# MAGIC STEP 6: Add silver table queries as tasks	43
# MAGIC STEP 7: Add gold table query as tasks	44
# MAGIC STEP 8: Add alert as a task	46
# MAGIC STEP 9: Add dashboard as a task	47
# MAGIC STEP 10: Run the workflow	48
# MAGIC View Lineage Graphs for each Table	51
# MAGIC STEP 1: Navigate back the the data explorer	52
# MAGIC STEP 2: View lineage diagrams	53
# MAGIC
# MAGIC
# MAGIC Prerequisites
# MAGIC
# MAGIC Make sure you can log into e2-field-eng.
# MAGIC Enable the new UI.
# MAGIC
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Explore SQL Warehouses
# MAGIC
# MAGIC SQL warehouses are the compute resources that let you run SQL commands on data objects within Databricks SQL. 
# MAGIC
# MAGIC In this lab you will be using a SQL Warehouse prepared by the instructor.
# MAGIC
# MAGIC But let's explore the process of creating a SQL warehouse so you're familiar with the steps.
# MAGIC  
# MAGIC 1.	In the left navigation pane, select SQL Warehouses.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Here is a list of all currently created SQL warehouses in your workspace. 
# MAGIC
# MAGIC 2.	Click Create SQL warehouse.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 3.	Note the following options for creating a SQL warehouse:
# MAGIC 1.	Cluster size - This is a “T-shirt size” representing the number of workers on the warehouse and the size of the driver. Larger T-shirt sizes should be used for larger datasets and more complex queries.
# MAGIC 2.	Auto stop - When a warehouse has been idle, an admin can have a warehouse automatically stop to save costs. We recommend 5-10 minutes as a reasonable balance between leaving warehouses on and turning them off to aggressively.
# MAGIC 3.	Scaling - This is how many clusters there are on a warehouse. More clusters equates to more available concurrency and throughput, which is useful for multi-user BI workloads.
# MAGIC 4.	Type - Always use serverless if you can.
# MAGIC
# MAGIC For more information on configuring a SQL warehouse, click here. 
# MAGIC  
# MAGIC
# MAGIC Explore the data 
# MAGIC
# MAGIC The Data Explorer provides a space to explore and manage data, schemas (databases), tables, and permissions. 
# MAGIC
# MAGIC The data explorer is the main UI for the Unity Catalog object model. Here, you can view schema details, preview sample data, and see table details and properties. 
# MAGIC
# MAGIC Administrators can change owners and grant and revoke permissions to data.
# MAGIC
# MAGIC STEP 1: Navigate to the Data Explorer
# MAGIC
# MAGIC 1.	In the left navigation pane, select Data. 
# MAGIC  
# MAGIC
# MAGIC STEP 2: Create a catalog
# MAGIC
# MAGIC A catalog is the topmost layer of the Unity Catalog hierarchy and is used to organize your schemas. 
# MAGIC
# MAGIC You can create a catalog through the Data Explorer or a SQL command. 
# MAGIC
# MAGIC To create via the Data Explorer, click Create catalog in the upper right corner. 
# MAGIC  
# MAGIC
# MAGIC 1.	For Catalog name, use the format fs_<first initial><last name>. For example if your name is Jane Doe, use fs_jdoe.
# MAGIC 2.	Leave Managed location blank.
# MAGIC 3.	Enter a comment mentioning "#databucks" for some databucks.
# MAGIC 4.	Click Create.
# MAGIC  
# MAGIC
# MAGIC You should now be back in the Data Explorer UI on your newly created catalog.
# MAGIC
# MAGIC STEP 3: Create a Schema
# MAGIC Now that you have a catalog, you will want to create a schema. A schema is used to logically organize tables, views, and other objects. 
# MAGIC
# MAGIC Did you know?
# MAGIC
# MAGIC In Unity Catalog, a schema is equivalent to a database in other systems. In fact, the two Databricks SQL keywords SCHEMA and DATABASE are interchangeable aliases. 
# MAGIC
# MAGIC To create a schema in the Data Explorer UI, click Create schema in the upper right hand corner.
# MAGIC  
# MAGIC
# MAGIC 1.	For Schema name enter tpch.
# MAGIC 2.	Leave Managed location blank.
# MAGIC 3.	Optionally enter a comment. (This is not required for the lab.)
# MAGIC 4.	Click Create.
# MAGIC  
# MAGIC
# MAGIC You will now see your new schema in the catalog.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Write SQL Queries
# MAGIC
# MAGIC Once your SQL warehouse, catalog, and schema are created, you are ready to start writing SQL queries.
# MAGIC
# MAGIC STEP 1: Navigate to the SQL Editor
# MAGIC
# MAGIC The SQL editor allows you to query data, view data in the schema browser, build visualizations, and configure alerts. 
# MAGIC
# MAGIC 1.	In the left navigation pane, select SQL Editor.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC The SQL Editor consists of 3 main panes:
# MAGIC ●	A schema browser on the left which allows you to explore tables and views within Unity Catalog
# MAGIC ●	The query pane, where you can write, run, save, and share queries
# MAGIC ●	The results pane where you can view the results of your queries, as well as create additional visualizations based on your query results
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Setting the Catalog
# MAGIC
# MAGIC 1.	In the catalog selector dropdown within your New Query tab, select your catalog (e.g. fs_jdoe).
# MAGIC
# MAGIC Create Bronze Tables
# MAGIC
# MAGIC STEP 1: Use the COPY INTO command to ingest our first table
# MAGIC
# MAGIC To ingest data from your cloud object store, or from Databricks sample data, use the COPY INTO command.
# MAGIC
# MAGIC COPY INTO is an idempotent operation 
# MAGIC Supported data sources: CSV, JSON, Avro, Parquet, text, binary 
# MAGIC See: COPY INTO DBSQL Tutorial
# MAGIC
# MAGIC 1.	Copy and paste the SQL command below into the SQL editor:
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tpch.orders_bronze;
# MAGIC
# MAGIC COPY INTO tpch.orders_bronze FROM 
# MAGIC       (SELECT 
# MAGIC                     _c0 O_ORDERKEY,
# MAGIC 				_c1 O_CUSTKEY, 
# MAGIC 				_c2 O_ORDERSTATUS, 
# MAGIC 				_c3 O_TOTALPRICE, 
# MAGIC 				_c4 O_ORDER_DATE, 
# MAGIC 				_c5 O_ORDERPRIORITY,
# MAGIC 				_c6 O_CLERK, 
# MAGIC 				_c7 O_SHIPPRIORITY, 
# MAGIC 				_c8 O_COMMENT  
# MAGIC FROM 'dbfs:/databricks-datasets/tpch/data-001/orders/')
# MAGIC FILEFORMAT = CSV 
# MAGIC FORMAT_OPTIONS('header' = 'false',
# MAGIC 			'inferSchema' = 'true',
# MAGIC 			'delimiter' = '|')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC
# MAGIC In this lab you are reading sample data from the Databricks File System (DBFS). In practice, this path can be replaced with your cloud storage path.
# MAGIC
# MAGIC You can set the schema of your table within the COPY INTO command as seen in the query. 
# MAGIC
# MAGIC
# MAGIC Wait a second …
# MAGIC
# MAGIC While Databricks SQL uses schema as a logical hierarchy for organizing tables, the word "schema" is also commonly used to refer to the column names and data types in a table. So you can define your table's schema, and then put it in your schema!
# MAGIC
# MAGIC There are different options you can set for FORMAT_OPTIONS and COPY_OPTIONS to control the ingestion operation. 
# MAGIC
# MAGIC 2.	Click Run all (1000) to run the query and view your results. 
# MAGIC  
# MAGIC 3.	Run the query again. Note this time results show 0 inserted rows. This is because COPY INTO commands keep track of which source files have been already ingested.
# MAGIC
# MAGIC Note: make sure to give each query an identifying name, as well as save the query after any changes have been made.
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC STEP 2: Ingest remaining table
# MAGIC Next, you will ingest your other source table, customer.
# MAGIC
# MAGIC 1.	Create a new query.
# MAGIC  
# MAGIC 2.	Copy and paste the below SQL command into the editor:
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tpch.customer_bronze;
# MAGIC
# MAGIC COPY INTO tpch.customer_bronze FROM 
# MAGIC (SELECT 
# MAGIC                       _c0 C_CUSTKEY,
# MAGIC 				_c1 C_NAME, 
# MAGIC 				_c2 C_ADDRESS, 
# MAGIC 				_c3 C_NATIONKEY, 
# MAGIC 				_c4 C_PHONE, 
# MAGIC 				_c5 C_ACCTBAL,
# MAGIC 				_c6 C_MKTSEGMENT, 
# MAGIC 				_c7 C_COMMENT
# MAGIC FROM 'dbfs:/databricks-datasets/tpch/data-001/customer/')
# MAGIC FILEFORMAT = CSV 
# MAGIC FORMAT_OPTIONS('header' = 'false',
# MAGIC 		    'inferSchema' = 'true',
# MAGIC 		    'delimiter' = '|')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC 3.	Run the query to view the results.
# MAGIC 4.	Rename the query tpch_customer_bronze.
# MAGIC 5.	Save your query.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC STEP 3: View the new tables
# MAGIC These two new tables you have created are your initial bronze layer. 
# MAGIC
# MAGIC The bronze layer is raw data that retains the full, unprocessed history of each dataset.
# MAGIC
# MAGIC See: What is the medallion lakehouse architecture?
# MAGIC
# MAGIC Let's use the Data Explorer to view your tables in more detail.
# MAGIC
# MAGIC 1.	In the left navigation pane, select Data.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC STEP 4: Explore the tables
# MAGIC
# MAGIC If you click on a table in the Data Explorer, you can see more information about the columns, preview some sample data, view details, view and create permissions, as well as history and lineage.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Create Silver Tables
# MAGIC
# MAGIC Silver tables represent a validated, enriched version of the data from your bronze layer. Here, you are enriching your bronze data by removing null data values and applying business logic as filters to some columns.
# MAGIC
# MAGIC STEP 1: Create silver tables using CTAS statements
# MAGIC
# MAGIC You will use the CREATE TABLE AS SELECT (CTAS) SQL syntax to create new silver tables directly from your bronze layer.
# MAGIC
# MAGIC 1.	In the left navigation pane, select SQL Editor.
# MAGIC 2.	Create a new query.
# MAGIC 3.	Copy and paste the SQL command below.
# MAGIC 4.	Run the query.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE tpch.orders_silver AS
# MAGIC
# MAGIC SELECT * FROM tpch.orders_bronze
# MAGIC WHERE O_ORDERKEY IS NOT NULL
# MAGIC AND O_ORDER_DATE >= date '1990-01-01';
# MAGIC
# MAGIC SELECT COUNT(DISTINCT O_ORDERKEY) FROM tpch.orders_silver;
# MAGIC  
# MAGIC
# MAGIC CREATE OR REPLACE TABLE tpch.customer_silver AS 
# MAGIC SELECT C_CUSTKEY, C_NAME, C_NATIONKEY, C_ACCTBAL
# MAGIC FROM tpch.customer_bronze
# MAGIC WHERE C_CUSTKEY IS NOT NULL
# MAGIC AND C_NATIONKEY != 21;
# MAGIC
# MAGIC SELECT COUNT(DISTINCT C_CUSTKEY) FROM tpch.customer_silver;
# MAGIC  
# MAGIC
# MAGIC
# MAGIC Create Visualizations
# MAGIC From these tables, you can use SELECT queries to view data and create visualizations.
# MAGIC
# MAGIC STEP 1: Write a SELECT statement to get the count of all orders
# MAGIC
# MAGIC 1.	Navigate to the tpch_orders_silver query.
# MAGIC
# MAGIC The SELECT statement is what you will use for your visualization.
# MAGIC  
# MAGIC
# MAGIC STEP 2: Create the visualization
# MAGIC
# MAGIC 1.	Click the + button in the results pane ribbon, then select Visualization.
# MAGIC  
# MAGIC
# MAGIC You will be taken to the visualization creator UI where you can select from a variety of customizable visualization types.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC For this query, you will create a counter of the number of orders. 
# MAGIC
# MAGIC 2.	For Visualization type choose Counter.
# MAGIC 3.	For Label enter Number of Orders.
# MAGIC 4.	For Value column choose count(DISTINCT O_ORDERKEY).
# MAGIC
# MAGIC You can see a preview of your visualization which changes as you select different options.
# MAGIC  
# MAGIC
# MAGIC 5.	 Click Save.
# MAGIC  
# MAGIC
# MAGIC 6.	Save the query.
# MAGIC
# MAGIC Your new visualization is now part of your query results.
# MAGIC  
# MAGIC
# MAGIC
# MAGIC STEP 3: Make a query and visualization for Total Price
# MAGIC
# MAGIC Let's make another visualization reflecting the total price of all of your orders. 
# MAGIC
# MAGIC First let's create a new query for your visualization.
# MAGIC
# MAGIC 1.	Create a new query.
# MAGIC 2.	Copy and paste the following SQL command into your query:
# MAGIC
# MAGIC SELECT O_ORDERKEY, 
# MAGIC        O_TOTALPRICE 
# MAGIC FROM tpch.orders_silver;
# MAGIC 3.	Run the query.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 4.	Create a new visualization.
# MAGIC 5.	Name it Order Amount.
# MAGIC 6.	For Visualization type choose Box.
# MAGIC 7.	Check Horizontal chart.
# MAGIC 8.	For X column choose O_TOTALPRICE.
# MAGIC 9.	Click Save.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 10.	Name your query Total Price.
# MAGIC 11.	Save your query.
# MAGIC
# MAGIC Create Gold Tables
# MAGIC
# MAGIC Now you will create two gold tables. These tables are refined and aggregated data tables.
# MAGIC
# MAGIC STEP 1: Make gold table for order status
# MAGIC
# MAGIC Your gold table will show the number or orders in each of the statuses (F, O, P).
# MAGIC
# MAGIC 1.	Create a new query.
# MAGIC 2.	Name the query Order Status.
# MAGIC 3.	Copy and paste the SQL command below into your query.
# MAGIC 4.	Run the query.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE tpch.order_status AS
# MAGIC SELECT COUNT(O_ORDERSTATUS) AS ORDERSTATUS_COUNT, 
# MAGIC 		   O_ORDERSTATUS 
# MAGIC FROM tpch.orders_silver
# MAGIC GROUP BY O_ORDERSTATUS;
# MAGIC
# MAGIC SELECT * FROM tpch.order_status;
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC STEP 4: Make visualization for the order status table
# MAGIC
# MAGIC Now you can make a visualization to show the distribution of the different order statuses using a histogram. 
# MAGIC
# MAGIC 1.	Create a new visualization.
# MAGIC 2.	Rename it to Order Status.
# MAGIC 3.	Set Visualization type to Bar.
# MAGIC 4.	Set X column to O_ORDERSTATUS.
# MAGIC 5.	Set Y column to ORDERSTATUS_COUNT.
# MAGIC 6.	Navigate to the X axis tab.
# MAGIC 7.	Set Name to Order Status.
# MAGIC 8.	Navigate to the Y axis tab.
# MAGIC 9.	Set Name to Number of Orders.
# MAGIC 10.	Click Save.
# MAGIC 11.	Save your query.
# MAGIC
# MAGIC  
# MAGIC  
# MAGIC  
# MAGIC
# MAGIC Unified Visual Analytics with DBSQL Dashboards
# MAGIC
# MAGIC Now that you have all of your tables and visualizations, you can make a dashboard to unify your visualizations in a single view.
# MAGIC
# MAGIC 1.	In the left navigation pane, select Dashboards.
# MAGIC
# MAGIC Here, you can view all the dashboards you have access to. You can add dashboards to your favorites, and tag dashboards to help with discovery and classification.
# MAGIC
# MAGIC STEP 1: Make a new dashboard
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 1.	Click Create Dashboard. 
# MAGIC  
# MAGIC
# MAGIC 2.	Set Dashboard name to fs_dash_<first initial><last name>.
# MAGIC 3.	Click Save.
# MAGIC  
# MAGIC
# MAGIC A blank dashboard will now appear. 
# MAGIC  
# MAGIC
# MAGIC STEP 2: Add visualizations to your dashboard
# MAGIC
# MAGIC There are two ways to add visualizations to the dashboard. Let's try them both.
# MAGIC
# MAGIC Method 1: Add from the dashboard 
# MAGIC 1.	In the Dashboard editor click Add.
# MAGIC  
# MAGIC 2.	Select Visualization.
# MAGIC  
# MAGIC
# MAGIC The Add visualization widget will appear. 
# MAGIC
# MAGIC You can only add visualizations from existing saved queries.
# MAGIC
# MAGIC 3.	From the query dropdown select tpch_orders_silver.
# MAGIC 4.	Choose Select existing visualization. A new dropdown will appear to select your visualization.
# MAGIC 5.	Choose Number of Orders.
# MAGIC 6.	Set Title to Number of Orders.
# MAGIC 7.	Optionally enter a Description.
# MAGIC 8.	Click Add to dashboard.
# MAGIC  
# MAGIC
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Your visualization is now on the dashboard. Click and drag it to move it around or resize it. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Method 2: From the SQL Editor
# MAGIC
# MAGIC Another way you can add visualizations to your dashboard is directly from your queries in the SQL editor.
# MAGIC
# MAGIC 1.	In the left navigation pane, select SQL Editor.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Let’s add the Order Amount visualization.
# MAGIC 1.	Open the Total Price query.
# MAGIC
# MAGIC  
# MAGIC 2.	In the results pane ribbon, click the down arrow (⌄) next to the name of your Order Amount visualization. 
# MAGIC  
# MAGIC 3.	Click Add to dashboard. 
# MAGIC  
# MAGIC
# MAGIC The Add to dashboard UI should appear.
# MAGIC 4.	In the dropdown search for your dashboard (e.g. fs_dash_jdoe).
# MAGIC 5.	Click Add.
# MAGIC  
# MAGIC
# MAGIC STEP 3: Add remaining visualizations
# MAGIC Using either of the above techniques, add your Order Status visualization from the Order Status query to the dashboard.
# MAGIC
# MAGIC Navigate back to your dashboard. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Here, you can rearrange and resize your visualizations to make the dashboard appear how you want. To do this, click the “⋮” button in the upper right corner, and select ‘Edit’. 
# MAGIC
# MAGIC Note: here you also see other options, such as export dashboard, and download as PDF that can be utilized. 
# MAGIC
# MAGIC Once in edit mode, you can resize and rearrange your visualizations. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Create Alerts
# MAGIC
# MAGIC Alerts can be set up to notify you when a field returned by a scheduled query meets or exceeds a threshold. 
# MAGIC
# MAGIC Alerts are useful for checking for data errors or anomalies, as well as standard status reporting on pipelines.
# MAGIC
# MAGIC In this lab you will create an alert when your number of customers goes above 800,000.
# MAGIC
# MAGIC STEP 1: Create an alert
# MAGIC
# MAGIC 1.	In the left navigation pane, select Alerts.
# MAGIC  
# MAGIC
# MAGIC 2.	Click Create alert in the upper right corner. 
# MAGIC  The New alert UI should appear.
# MAGIC 3.	Name the alert Number of Customers.
# MAGIC 4.	Set Query to tpch_customer_silver.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 5.	In the Trigger condition section:
# MAGIC a.	Set Value column to count(DISTINCT C_CUSTKEY).
# MAGIC b.	Set Operator to >.
# MAGIC c.	Set Threshold value to 800000.
# MAGIC 6.	Confirm the following setting values, which should be set by default:
# MAGIC a.	When triggered, send notification set to Just once
# MAGIC b.	Template set to Use default template
# MAGIC c.	Refresh set to Every 1 hour
# MAGIC 7.	Click Create alert.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC STEP 1: Save all queries
# MAGIC Before getting started with workflows, make sure all of your queries in the SQL editor are saved. If a query has not been saved since the last edit, a ‘*’ will appear next to the save button.
# MAGIC
# MAGIC In this case, click Save to save the query.
# MAGIC  
# MAGIC
# MAGIC Orchestrating SQL with Databricks Workflows
# MAGIC
# MAGIC Workflows are great.
# MAGIC
# MAGIC In this section you are going to create a workflow to convert your ingestion and transformation queries into an orchestrated pipeline you can run any time you want to process new data.
# MAGIC
# MAGIC STEP 2: Navigate to workflows
# MAGIC
# MAGIC 1.	In the left navigation pane, select the persona dropdown (currently set to SQL.)
# MAGIC 2.	Select the Data Science and Engineering persona. 
# MAGIC  
# MAGIC
# MAGIC 3.	Select Workflows.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC STEP 3: Create workflow job
# MAGIC
# MAGIC 1.	Click Create job. 
# MAGIC  
# MAGIC
# MAGIC 2.	Rename your job by clicking on Add a name for your job at the top. 
# MAGIC  
# MAGIC
# MAGIC Tasks
# MAGIC
# MAGIC A job consists of one or more tasks. In this lab you will be using your previously created SQL queries, alert, and dashboard as tasks.
# MAGIC
# MAGIC STEP 4: Add task for ingesting TPCH Orders into your bronze layer 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 1.	Set Task name to Create_orders_bronze.
# MAGIC 2.	Set Type to SQL. The UI will update with new fields.
# MAGIC 3.	Set SQL Task to Query.
# MAGIC 4.	Set SQL Query to tpch_orders_bronze. (If you saved your query under a different name, select that instead.)
# MAGIC 5.	Set SQL Warehouse to Shared Flight School.
# MAGIC 6.	Click Create.
# MAGIC
# MAGIC The task will appear in the job’s DAG UI above.
# MAGIC
# MAGIC Try it yourself!
# MAGIC
# MAGIC An email can optionally be added to receive notifications when this task starts, succeeds, or fails. 
# MAGIC
# MAGIC This task can also be set up to retry a specified number of times. 
# MAGIC
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC STEP 5: Add remaining bronze table queries as tasks
# MAGIC To add a new task to your job, click the large + button in the center.
# MAGIC  
# MAGIC
# MAGIC Did you notice?
# MAGIC
# MAGIC Once you add multiple tasks to your job, a Depends on option appears in your task creation UI. This is where you would add any dependencies of one task on another. 
# MAGIC
# MAGIC In this lab, your bronze queries have no dependencies so for now leave this field blank.
# MAGIC
# MAGIC 1.	Add a new task.
# MAGIC 2.	Set Task name to Create_customer_bronze.
# MAGIC 3.	Set Type to SQL. The UI will update with new fields.
# MAGIC 4.	Set SQL Task to Query.
# MAGIC 5.	Set SQL Query to tpch_customer_bronze. (If you saved your query under a different name, select that instead.)
# MAGIC 6.	Set SQL Warehouse to Shared Flight School.
# MAGIC 7.	Click Create.
# MAGIC
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC STEP 6: Add silver table queries as tasks
# MAGIC Now you will add your silver table queries. 
# MAGIC
# MAGIC These queries depend on the result of their bronze counterpart. So, when creating these queries, you will add the corresponding bronze table query to the Depends on field. 
# MAGIC
# MAGIC 1.	Add a new task
# MAGIC 2.	Set Task name to Create_orders_silver.
# MAGIC 3.	Set Type to SQL. The UI will update with new fields.
# MAGIC 4.	Set SQL Task to Query.
# MAGIC 5.	Set SQL Query to tpch_orders_silver. (If you saved your query under a different name, select that instead.)
# MAGIC 6.	Set SQL Warehouse to Shared Flight School.
# MAGIC 7.	Set depends on to Create_orders_bronze.
# MAGIC 8.	Click Create task.
# MAGIC 9.	Add a new task.
# MAGIC 10.	Set Task name to Create_customer_silver.
# MAGIC 11.	Set Type to SQL. The UI will update with new fields.
# MAGIC 12.	Set SQL Task to Query.
# MAGIC 13.	Set SQL Query to tpch_customer_silver. (If you saved your query under a different name, select that instead.)
# MAGIC 14.	Set SQL Warehouse to Shared Flight School.
# MAGIC 15.	Set depends on to Create_customer_bronze.
# MAGIC 16.	Click Create task.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC STEP 7: Add gold table queries as tasks
# MAGIC
# MAGIC Now let’s add in your gold table queries. 
# MAGIC
# MAGIC Order Status and Total Price only depend on the orders_silver table.
# MAGIC
# MAGIC 1.	Add a new task.
# MAGIC 2.	Set Task name to Order_status.
# MAGIC 3.	Set Type to SQL. The UI will update with new fields.
# MAGIC 4.	Set SQL Task to Query.
# MAGIC 5.	Set SQL Query to Order Status. (If you saved your query under a different name, select that instead.)
# MAGIC 6.	Set SQL Warehouse to Shared Flight School.
# MAGIC 7.	Set depends on to Create_orders_silver.
# MAGIC 8.	Click Create task.
# MAGIC 9.	Add a new task.
# MAGIC 10.	Set Task name to Total_price.
# MAGIC 11.	Set Type to SQL. The UI will update with new fields.
# MAGIC 12.	Set SQL Task to Query.
# MAGIC 13.	Set SQL Query to Total Price. (If you saved your query under a different name, select that instead.)
# MAGIC 14.	Set SQL Warehouse to Shared Flight School.
# MAGIC 15.	Set depends on to Create_orders_silver.
# MAGIC 16.	Click Create task.
# MAGIC
# MAGIC
# MAGIC Order Status
# MAGIC  
# MAGIC
# MAGIC Total Price:
# MAGIC  
# MAGIC
# MAGIC
# MAGIC
# MAGIC STEP 8: Add alert as a task
# MAGIC
# MAGIC The alert you created can also be part of this workflow. 
# MAGIC
# MAGIC 1.	Add a new task.
# MAGIC 2.	Set Task name to Number_of_customers_alert.
# MAGIC 3.	Set Type to SQL.
# MAGIC 4.	Set SQL task to Alert.
# MAGIC 5.	Set SQL alert to Number of Customers.
# MAGIC 6.	Set Depends on to customer_silver.
# MAGIC 7.	Click Create task.
# MAGIC
# MAGIC  
# MAGIC STEP 9: Add dashboard as a task
# MAGIC
# MAGIC You can also add a task to refresh your dashboard. 
# MAGIC
# MAGIC 1.	Add a new task.
# MAGIC 2.	Set Task name to Refresh_tpch_dashboard.
# MAGIC 3.	Set Type to SQL. The UI will update with new fields.
# MAGIC 4.	Set SQL Task to Dashboard.
# MAGIC 5.	Set SQL Dashboard to fs_dash_jdoe. (If you saved your dashboard under a different name, select that instead.)
# MAGIC 6.	Set SQL Warehouse to Shared Flight School.
# MAGIC 7.	Set depends on to Order_status and Total_price.
# MAGIC 8.	Click Create task.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC STEP 10: Run the workflow 
# MAGIC
# MAGIC The final DAG for the job should look like this:
# MAGIC  
# MAGIC
# MAGIC 1.	Click Run now in the top right corner.
# MAGIC 2.	Navigate to the Runs tab to view the run progressing in the matrix view.
# MAGIC   
# MAGIC
# MAGIC This matrix view allows you to troubleshoot and repair job failures. As tasks succeed, they will turn green. If they fail, they will turn red. 
# MAGIC As you can see below, the first run fully succeeded and took 1m 25s. 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC If you click on a task, you can see additional information about the task, such as duration, a link back to the query, and information about lineage.
# MAGIC
# MAGIC 3.	Click on the orders_silver task and explore the detailed information provided.
# MAGIC  
# MAGIC
# MAGIC  
# MAGIC
# MAGIC For demonstration sake, here is what it would look like if a task were to fail. 
# MAGIC In this example, customer_silver failed. If you click on the failed task you get the error. In this case it was because the dependency was not set up so customer_silver depends on customer_bronze. 
# MAGIC   
# MAGIC
# MAGIC View Lineage Graphs for your project
# MAGIC
# MAGIC Back in the Data Explorer, you can view lineage graphs for each table to see all the upstream and downstream tables, as well as pipelines, queries, and other code where the table is used.
# MAGIC
# MAGIC 1.	In the left navigation pane, select Data.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 2.	Navigate to your catalog (e.g. fs_jdoe) 
# MAGIC 3.	Navigate to the tpch schema. Here you will see all of your tables.
# MAGIC
# MAGIC  
# MAGIC 4.	Click on the orders_silver table.
# MAGIC 5.	Select the Lineage tab. 
# MAGIC  
# MAGIC
# MAGIC Here you can see the upstream and downstream dependencies of your table. This includes other tables, notebooks, workflows, Delta Live Table pipelines, dashboards, and queries.
# MAGIC
# MAGIC 6.	Click on See lineage graph to get a graphical view of the upstream and downstream tables.
# MAGIC  
# MAGIC
# MAGIC
