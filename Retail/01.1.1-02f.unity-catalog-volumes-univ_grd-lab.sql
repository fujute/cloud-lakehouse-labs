-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Volumes Quickstart (SQL & Python)
-- MAGIC
-- MAGIC This notebook provides an example workflow for creating your first Volume in Unity Catalog:
-- MAGIC
-- MAGIC - Choose a catalog and a schema, or create new ones.
-- MAGIC - Create a managed Volume under the chosen schema.
-- MAGIC - Browse the Volume using the three-level namespace.
-- MAGIC - Manage the Volume's access permissions.
-- MAGIC
-- MAGIC ## Version
-- MAGIC - 0.2f
-- MAGIC - fujuOrg
-- MAGIC
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC - Your workspace must be attached to a Unity Catalog metastore. See https://docs.databricks.com/data-governance/unity-catalog/get-started.html.
-- MAGIC - Your notebook is attached to a cluster that uses DBR 13.2+ and uses the single user or shared cluster access mode.
-- MAGIC
-- MAGIC ## Volumes under the Unity Catalog's three-level namespace
-- MAGIC
-- MAGIC Unity Catalog provides a three-level namespace for organizing data. To refer to a Volume, use the following syntax:
-- MAGIC
-- MAGIC `<catalog>.<schema>.<volume>`
-- MAGIC
-- MAGIC in this sample we will use  the following deails for catalog and schema
-- MAGIC - catalog_name = training_catalog
-- MAGIC - db_name = fukiat_julnual_isxj_da_asp (PLEASE CHANGE THE SCHEMA NAME to YOUR)
-- MAGIC
-- MAGIC ## References and  more information
-- MAGIC
-- MAGIC This notebook has been developed by referencing the sample provided in the Databricks documentation on Unity Catalog Volumes, available at https://docs.databricks.com/en/connect/unity-catalog/volumes.html.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![FUJUOrG](https://www.fuju.org/wp-content/uploads/2017/08/20180806_102329787_1920x1080-840x200.jpg "FUJUOrG")
-- MAGIC
-- MAGIC #Senario 1, task 1
-- MAGIC
-- MAGIC For this lab, participants will have two options to choose from: SQL or Python. Regardless of the chosen option, the following tasks must be completed:
-- MAGIC
-- MAGIC 1. **Data Acquisition**: Download the UTF-8 encoded dataset from the provided URL: https://data.go.th/en/dataset/univ_grd_11_01
-- MAGIC
-- MAGIC 2. **Data Upload**: Upload the downloaded dataset to the previously created volume using the web interface.
-- MAGIC
-- MAGIC 3. **Data Reading**: Read the uploaded file while ensuring the correct encoding is applied.
-- MAGIC
-- MAGIC 4. **Data Storage**: Write the data from the file to a Delta table.
-- MAGIC
-- MAGIC 5. **Data Visualization**: Read the data from the Delta table and generate a simple chart or plot to visualize the information.
-- MAGIC
-- MAGIC Participants are expected to demonstrate proficiency in completing these tasks, either through SQL or Python, showcasing their ability to work with data acquisition, storage, and visualization techniques.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Option 1: SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog_name="f11l_catalog" # please chage to your catalog 
-- MAGIC db_name="db_isxj_da_asp" # please change to your schame / db 
-- MAGIC table_name="univ_grd_tab"
-- MAGIC volume_name="quickstart_volume"
-- MAGIC
-- MAGIC spark.conf.set("cnf.catalog_name", catalog_name)
-- MAGIC spark.conf.set("cnf.db_name", db_name)
-- MAGIC spark.conf.set("cnf.table_name", table_name)
-- MAGIC
-- MAGIC table_full_path = catalog_name + "." + db_name + "." + table_name;
-- MAGIC display(table_full_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the current catalog.
-- MAGIC spark.sql("USE CATALOG ${cnf.catalog_name}")
-- MAGIC # Set the current catalog.
-- MAGIC spark.sql("USE SCHEMA ${cnf.db_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Show the current database (also called a schema).
-- MAGIC spark.catalog.currentDatabase()

-- COMMAND ----------

select current_catalog(),current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("DESCRIBE SCHEMA EXTENDED  ${cnf.db_name}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. **Data Acquisition**: 
-- MAGIC Download the UTF-8 encoded dataset from the provided URL: https://data.mhesi.go.th/dataset/9b6be911-8192-472d-91cc-f50676f0ffb7/resource/e2a37423-f025-4580-96ca-3fc5d70c3af5/download/univ_grd_11_05_2564.csv 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. **Data Upload**: 
-- MAGIC Upload the downloaded dataset( univ_grd_11_01.csv) to the previously created volume using the web interface.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC download_url = "https://data.mhesi.go.th/dataset/9b6be911-8192-472d-91cc-f50676f0ffb7/resource/e2a37423-f025-4580-96ca-3fc5d70c3af5/download/univ_grd_11_05_2564.csv"
-- MAGIC file_name = "univ_grd_11_05_2564.csv"
-- MAGIC path_volume = "/Volumes/" + catalog_name + "/" + db_name + "/" + volume_name
-- MAGIC print(path_volume) # Show the complete path
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp(f"{download_url}", f"{path_volume}/{file_name}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## 3. **Data Reading**: 
-- MAGIC Read the uploaded file while ensuring the correct encoding is applied.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Access Files in Volumes
-- MAGIC
-- MAGIC You can use dbutils, shell commands or local file system APIs to manage files stored in a Volume
-- MAGIC

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##### dbutils.fs
-- MAGIC You can use any of the <a href="https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs">dbutils file system utilities</a>, except for the mounts-related utilities. 
-- MAGIC
-- MAGIC We show some examples:
-- MAGIC - create a directory inside a Volume
-- MAGIC - copy a file from a another location in this directory
-- MAGIC - list the directory and check that the file is shown inside

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/Volumes/f11l_catalog/ktb_isxj_da_asp/quickstart_volume/")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Temp View
-- MAGIC

-- COMMAND ----------

-- SELECT * FROM csv.`dbfs:/Volumes/training_catalog/fukiat_julnual_isxj_da_asp/quickstart_volume/univ_grd_11_01.csv` limit 10;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW temp_view
USING csv
OPTIONS (
  path "dbfs:/Volumes/f11l_catalog/ktb_isxj_da_asp/quickstart_volume/univ_grd_11_05_2564.csv",
  header "true",
  encoding "UTF-8"
);

SELECT * FROM temp_view limit 10;

-- COMMAND ----------

SELECT UNIV_NAME_TH, SUM(AMOUNT)
FROM temp_view
WHERE AYEAR = "2564"
GROUP BY UNIV_NAME_TH
ORDER BY SUM(AMOUNT) DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. **Data Storage**: 
-- MAGIC Write the data from the file to a Delta table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Prepare Delta Table for reporting 

-- COMMAND ----------

select current_catalog(),current_database()

-- COMMAND ----------

-- Create the table using the data from another table, if it doesn't exist
CREATE TABLE IF NOT EXISTS f11l_catalog.db_isxj_da_asp.${cnf.table_name} AS SELECT * FROM temp_view;

-- COMMAND ----------

DESCRIBE TABLE EXTENDED  ${cnf.catalog_name}.${cnf.db_name}.${cnf.table_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Displaying table details 
-- MAGIC Displaying table details and file locations enables data exploration, management, and adherence to governance policies within chosen data processing frameworks.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## CHANGE S3 location to your location that you got from row #23 from previous step
-- MAGIC ## files = dbutils.fs.ls("s3://databricks-e2demofieldengwest/b169b504-4c54-49f2-bc3a-adf4b128f36d/tables/798257bf-06d8-46a9-ae69-c6154d0c7810")
-- MAGIC ## display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. **Data Visualization**: 
-- MAGIC Read the data from the Delta table and generate a simple chart or plot to visualize the information.

-- COMMAND ----------

SELECT UNIV_NAME_TH, SUM(AMOUNT)
FROM ${cnf.catalog_name}.${cnf.db_name}.${cnf.table_name}
WHERE AYEAR = "2564"
GROUP BY UNIV_NAME_TH
ORDER BY SUM(AMOUNT) DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Option 2: Python
-- MAGIC
-- MAGIC ####Reference 
-- MAGIC https://docs.databricks.com/en/getting-started/dataframes.html#language-Python
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog_name="f11l_catalog" # please chage to your catalog 
-- MAGIC db_name="db_isxj_da_asp" # please change to your schame / db 
-- MAGIC table_name="univ_grd_tab_02"
-- MAGIC
-- MAGIC spark.conf.set("cnf.catalog_name", catalog_name)
-- MAGIC spark.conf.set("cnf.db_name", db_name)
-- MAGIC spark.conf.set("cnf.table_name", table_name)
-- MAGIC
-- MAGIC
-- MAGIC table_full_path = catalog_name + "." + db_name + "." + table_name;
-- MAGIC display(table_full_path)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the current catalog.
-- MAGIC spark.sql("USE CATALOG ${cnf.catalog_name}")
-- MAGIC # Set the current catalog.
-- MAGIC spark.sql("USE SCHEMA ${cnf.db_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Show the current database (also called a schema).
-- MAGIC spark.catalog.currentDatabase()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("DESCRIBE SCHEMA EXTENDED  ${cnf.db_name}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. **Data Acquisition**: 
-- MAGIC Download the TIS-620 encoded dataset from the provided URL: https://data.go.th/en/dataset/univ_grd_11_01
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Data file
-- MAGIC The "univ_grd_11_01.csv"  can be found at	https://data.go.th/en/dataset/univ_grd_11_01
-- MAGIC And please upload the CSV file to the volume via UI.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##2. **Data Upload**: 
-- MAGIC Upload the downloaded dataset to the previously created volume using the web interface.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. **Data Reading**: 
-- MAGIC Read the uploaded file while ensuring the correct encoding is applied.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # spark is from the previous example
-- MAGIC sc = spark.sparkContext
-- MAGIC
-- MAGIC # A text dataset is pointed to by path.
-- MAGIC # The path can be either a single text file or a directory of text files
-- MAGIC path = "/Volumes/"+catalog_name+"/"+db_name+"/"+"quickstart_volume/univ_grd_11_05_2564.csv"
-- MAGIC
-- MAGIC df1 = spark.read.csv(path)
-- MAGIC df1.limit(10).show()
-- MAGIC # +-----------+
-- MAGIC # |      value|
-- MAGIC # +-----------+
-- MAGIC # |Michael, 29|
-- MAGIC # |   Andy, 30|
-- MAGIC # | Justin, 19|
-- MAGIC # +-----------+
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reading from UTF-8 Encoding

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read a csv with delimiter and a header
-- MAGIC df3 = spark.read.option("delimiter", ",").option("header", True).option("encoding","UTF-8").csv(path)
-- MAGIC df3.limit(10).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create a temporary view of the DataFrame:
-- MAGIC df3.createOrReplaceTempView("temp_view_name")
-- MAGIC # you can perform SQL queries on this view:
-- MAGIC df4=spark.sql("select * from temp_view_name limit 5")
-- MAGIC display(df4)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. **Data Storage**: 
-- MAGIC Write the data from the file to a Delta table.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(table_full_path)
-- MAGIC df3.write.mode("overwrite").saveAsTable(table_full_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. **Data Visualization**: 
-- MAGIC Read the data from the Delta table and generate a simple chart or plot to visualize the information.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Assuming 'table_name' variable holds the name of your table
-- MAGIC table_name = "univ_grd_tab_02"  # replace with your table name
-- MAGIC
-- MAGIC # Construct the SQL query as a string
-- MAGIC query = f"SELECT * FROM {table_name}"
-- MAGIC
-- MAGIC # Execute the query
-- MAGIC df4 = spark.sql(query)
-- MAGIC
-- MAGIC # Display the results
-- MAGIC df4.limit(10).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Gernerate python code SQL with Databricks assistant

-- COMMAND ----------

-- SQL CODE 
-- SELECT UNIV_NAME_TH, SUM(AMOUNT)
-- FROM ${cnf.catalog_name}.${cnf.db_name}.${cnf.table_name}
-- WHERE AYEAR = "2564"
-- GROUP BY UNIV_NAME_TH
-- ORDER BY SUM(AMOUNT) DESC
-- LIMIT 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC df5 = spark.table('${cnf.catalog_name}.${cnf.db_name}.${cnf.table_name}')
-- MAGIC result = df5.filter(df5.AYEAR == "2564") \
-- MAGIC     .groupBy("UNIV_NAME_TH") \
-- MAGIC     .agg(F.sum("AMOUNT").alias("SUM_AMOUNT")) \
-- MAGIC     .orderBy("SUM_AMOUNT", ascending=False) \
-- MAGIC     .limit(10) \
-- MAGIC     .select("UNIV_NAME_TH", "SUM_AMOUNT")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC result.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optional : Create View
-- MAGIC reference: https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-views  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog_name="f11l_catalog"
-- MAGIC db_name="db_isxj_da_asp"
-- MAGIC table_name="univ_grd_tab_02"
-- MAGIC view_name="univ_grd_view_2562"
-- MAGIC
-- MAGIC spark.conf.set("cnf.catalog_name", catalog_name)
-- MAGIC spark.conf.set("cnf.db_name", db_name)
-- MAGIC spark.conf.set("cnf.table_name", table_name)
-- MAGIC spark.conf.set("cnf.view_name",view_name)
-- MAGIC
-- MAGIC table_full_path = catalog_name + "." + db_name + "." + table_name;
-- MAGIC display(table_full_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table(table_name)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${cnf.catalog_name}.${cnf.db_name}.${cnf.view_name} AS
SELECT
  UNIV_NAME_TH,
  SUM(AMOUNT) AS AMOUNT
FROM ${cnf.catalog_name}.${cnf.db_name}.${cnf.table_name}
WHERE AYEAR = 2562
GROUP BY UNIV_NAME_TH;

-- df.createOrReplaceTempView("temp_view_name")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql("DESCRIBE TABLE EXTENDED ${cnf.catalog_name}.${cnf.db_name}.${cnf.view_name}"))

-- COMMAND ----------

select * from ${cnf.view_name} ORDER BY AMOUNT DESC limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  Optional Spark session

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC
-- MAGIC # Create a SparkSession object
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC
-- MAGIC # Retrieve all configurations from SparkSession
-- MAGIC conf_vals = spark.sparkContext.getConf().getAll()
-- MAGIC
-- MAGIC # Display the configuration values
-- MAGIC for conf_name, conf_val in conf_vals:
-- MAGIC     print(conf_name, "=", conf_val)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert the configuration values to a DataFrame
-- MAGIC conf_df = spark.createDataFrame(conf_vals, ["Configuration", "Value"])
-- MAGIC
-- MAGIC # Display the DataFrame
-- MAGIC display(conf_df)
