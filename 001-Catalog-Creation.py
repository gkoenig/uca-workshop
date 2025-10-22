# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Lab: Environment Setup and Catalog Creation
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand Unity Catalog's three-level namespace
# MAGIC - Create catalogs, schemas, and volumes with proper governance
# MAGIC - Apply Unity Catalog permissions and security model
# MAGIC
# MAGIC ## Unity Catalog Architecture Overview
# MAGIC
# MAGIC **Three-Level Namespace**: `catalog.schema.table`
# MAGIC - **Catalog**: Top-level container for organizing data by environment/business unit
# MAGIC - **Schema**: Logical grouping within catalog (bronze/silver/gold layers)  
# MAGIC - **Table/Volume/View**: Actual data objects

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup and User Context
# MAGIC
# MAGIC Understanding current user context is crucial for Unity Catalog governance and creating unique object names.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Get current user context for governance and unique naming
w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = w.current_user.me().user_name.split('@')[0]

print(f"Current User: {user_name}")
print(f"User ID for naming: {user_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Catalog with Unity Catalog Best Practices
# MAGIC
# MAGIC **Catalog Naming Conventions**:
# MAGIC - Use descriptive names that indicate purpose or environment
# MAGIC - Include user/team identifier for training environments
# MAGIC - Consider data classification and access patterns
# MAGIC
# MAGIC **Using Your Catalog in the Lab**:
# MAGIC
# MAGIC A catalog has been created for you as part of this setup. However, you can also create catalogs programmatically using the following SQL command:
# MAGIC <br>
# MAGIC <br>
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS `your_catalog_name`
# MAGIC COMMENT 'Description of your catalog.'
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Reference your unique catalog name
catalog_name = user_id
print(catalog_name)

# COMMAND ----------

# DBTITLE 1,Set catalog context for subsequent operations
spark.sql(f"USE CATALOG `{catalog_name}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Exploring Unity Catalog Details
# MAGIC
# MAGIC Ways to review and access details about the unity catalog assets deployed - including current metastore and catalog in use, as well as leveraging the Information Schema

# COMMAND ----------

# DBTITLE 1,Verify catalog creation and examine properties
spark.sql(f"DESCRIBE CATALOG EXTENDED `{catalog_name}`").display()

# COMMAND ----------

# DBTITLE 1,Check Unity Catalog is enabled and get metastore information
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CURRENT_METASTORE() as metastore_id,
# MAGIC   CURRENT_CATALOG() as current_catalog,
# MAGIC   CURRENT_SCHEMA() as current_schema,
# MAGIC   CURRENT_USER() as current_user;

# COMMAND ----------

# DBTITLE 1,Get Catalog details via the Information Schema
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   catalog_name,
# MAGIC   catalog_owner,
# MAGIC   comment
# MAGIC FROM information_schema.catalogs
# MAGIC ORDER BY catalog_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Schema Structure
# MAGIC
# MAGIC **Unity Catalog Schema Organization**:
# MAGIC - **Sales Schema**: Raw data and transformations

# COMMAND ----------

# DBTITLE 1,Create Sales schema for our sales pipeline
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sales
# MAGIC COMMENT 'Sales schema to review customers and orders.';

# COMMAND ----------

# DBTITLE 1,Set schema context
# MAGIC %sql
# MAGIC USE SCHEMA sales;

# COMMAND ----------

# DBTITLE 1,Verify our current three-level namespace context
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CURRENT_CATALOG() as catalog,
# MAGIC   CURRENT_SCHEMA() as schema,
# MAGIC   CONCAT(CURRENT_CATALOG(), '.', CURRENT_SCHEMA()) as full_namespace;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Volume for File Storage
# MAGIC
# MAGIC **Unity Catalog Volumes**:
# MAGIC - Provide file system access within UC namespace
# MAGIC - Support structured and unstructured data
# MAGIC - Enable secure file sharing across workspaces
# MAGIC - Track lineage for file-based operations

# COMMAND ----------

# DBTITLE 1,Create volume for tracking and raw file storage.
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS tracking
# MAGIC COMMENT 'Volume for checkpoint files and schema tracking';

# COMMAND ----------

# DBTITLE 1,List volumes to confirm creation
spark.sql(f"SHOW VOLUMES IN `{catalog_name}`.sales").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC **What we accomplished:**
# MAGIC - ✅ **Environment Setup**: Verified Unity Catalog connectivity and user context
# MAGIC - ✅ **Three-Level Namespace**: Created `{catalog_name}.sales.{objects}` structure
# MAGIC - ✅ **Storage**: Created volume for file-based operations
# MAGIC - ✅ **Documentation**: Added comments for objects
# MAGIC
# MAGIC **Key Unity Catalog Concepts Demonstrated:**
# MAGIC - Metastore and workspace relationship
# MAGIC - Catalog and schema organization patterns
# MAGIC - Volume creation for file governance
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Data ingestion with Auto Loader
# MAGIC - Table creation and management
# MAGIC - Advanced security implementations
# MAGIC
# MAGIC **Objects Created:**
# MAGIC - Catalog: `{catalog_name}`
# MAGIC - Schemas: `{catalog_name}.sales`
# MAGIC - Volume: `{catalog_name}.sales.tracking`

# COMMAND ----------

print(f"✅ Lab 1 Complete. Catalog '{catalog_name}' ready for data pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussion Questions
# MAGIC
# MAGIC 1. **Why use three-level namespace?** How does `catalog.schema.table` improve data organization?
# MAGIC 2. **Permission Strategy**: When would you grant permissions at catalog vs schema vs table level?
# MAGIC 3. **Catalog Organization**: How would you organize catalogs for dev/staging/prod environments?
# MAGIC 4. **Volume Use Cases**: When would you use volumes vs tables for data storage?
# MAGIC 5. **Governance Benefits**: What governance challenges does Unity Catalog solve?
