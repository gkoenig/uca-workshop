# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Lab: Table Creation and Management
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Create both managed tables
# MAGIC - Explore table properties and metadata
# MAGIC - Understand DROP behavior
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Lab 1 & 2 (Catalog Creation + Configuration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set Up Environment
# MAGIC
# MAGIC Let's make sure we're in the right catalog and schema context.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Get current user context for governance and unique naming
w = WorkspaceClient()
user_name = w.current_user.me().user_name
user_id = w.current_user.me().user_name.split('@')[0]

print(f"Current User: {user_name}")
print(f"User ID for naming: {user_id}")

# COMMAND ----------

catalog_name = user_id
schema_name = "sales"

# COMMAND ----------

# Set catalog and schema context for subsequent operations
spark.sql(f"USE CATALOG `{catalog_name}`")
spark.sql(f"USE SCHEMA `{schema_name}`")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm current context
# MAGIC SELECT 
# MAGIC   CURRENT_CATALOG() as current_catalog,
# MAGIC   CURRENT_SCHEMA() as current_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Managed Tables
# MAGIC
# MAGIC Managed tables are fully controlled by Unity Catalog. The data is stored in managed storage locations.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE customers
              AS SELECT *
              FROM read_files('/Volumes/uc_wksp/gov_lab/files/customers/')""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE nations
              AS SELECT *
              FROM read_files('/Volumes/uc_wksp/gov_lab/files/nations/')""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE orders
              AS SELECT *
              FROM read_files('/Volumes/uc_wksp/gov_lab/files/orders/')""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE regions
              AS SELECT *
              FROM read_files('/Volumes/uc_wksp/gov_lab/files/regions/')""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE suppliers
              USING ICEBERG
              AS SELECT *
              FROM read_files('/Volumes/uc_wksp/gov_lab/files/suppliers/')""")

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.sales_fact AS
            SELECT 
              row_number() OVER (ORDER BY l.l_orderkey, l.l_linenumber) as sales_key,
              o.o_orderkey as order_key,
              l.l_linenumber as lineitem_key,
              c.c_custkey as customer_key,
              s.s_suppkey as supplier_key,
              o.o_orderdate as date_key,
              l.l_extendedprice as sales_amount,
              l.l_quantity as quantity
            FROM samples.tpch.lineitem l
            JOIN {catalog_name}.{schema_name}.orders o ON l.l_orderkey = o.o_orderkey
            JOIN {catalog_name}.{schema_name}.customers c ON o.o_custkey = c.c_custkey
            JOIN {catalog_name}.{schema_name}.suppliers s ON l.l_suppkey = s.s_suppkey
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data was inserted
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data was inserted
# MAGIC SELECT * FROM suppliers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compare Table Properties
# MAGIC
# MAGIC Let's examine the key differences between managed and external tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examine the managed Delta table structure
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examine the managed Iceberg table structure
# MAGIC DESCRIBE EXTENDED suppliers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table properties for Delta managed table
# MAGIC SHOW TBLPROPERTIES customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table properties for Iceberg managed table
# MAGIC SHOW TBLPROPERTIES suppliers;

# COMMAND ----------

# View table details from the information_schema
df = spark.sql(f"""
SELECT 
  table_catalog,
  table_schema,
  table_name,
  table_type,
  table_owner,
  created,
  last_altered,
  comment
FROM information_schema.tables
WHERE table_catalog = '{catalog_name}' 
  AND table_schema = '{schema_name}'
""")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Understanding DROP Behavior
# MAGIC
# MAGIC **⚠️ CAUTION: The following demonstrates DROP behavior. Be careful with DROP operations!**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP the managed table (this will delete metadata AND data)
# MAGIC DROP TABLE customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in our schema
# MAGIC SHOW TABLES LIKE 'customers';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show the recently dropped tables
# MAGIC UNDROP TABLE customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in our schema
# MAGIC SHOW TABLES LIKE 'customers';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP the managed table (this will delete metadata AND data)
# MAGIC DROP TABLE customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in our schema
# MAGIC SHOW TABLES LIKE 'customers';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all the dropped tables and see multiple drop records
# MAGIC SHOW TABLES DROPPED;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UNDROP the managed table by referencing the most recently deleted tableId from the above results
# MAGIC UNDROP TABLE WITH ID 'update_this_value_with_your_tableId';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in our schema
# MAGIC SHOW TABLES LIKE 'customers';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC **What you learned:**
# MAGIC - ✅ **Managed Tables**: Unity Catalog controls storage, simpler to manage, Delta + Iceberg format
# MAGIC - ✅ **DROP Behavior**: 
# MAGIC   - Managed: Deletes metadata AND data
# MAGIC - ✅ **Table Properties**: Use `DESCRIBE EXTENDED` and `SHOW TBLPROPERTIES`
# MAGIC
# MAGIC **Key Commands Used:**
# MAGIC - `CREATE TABLE ... USING DELTA` (managed)
# MAGIC - `DESCRIBE EXTENDED table_name`
# MAGIC - `SHOW TBLPROPERTIES table_name`
# MAGIC - `DROP`, `UNDROP` operations
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Start with managed tables for simplicity
# MAGIC - Curate metadata for tables at creation and within the UI
# MAGIC - Always test DROP operations in development first
# MAGIC
# MAGIC **Next Lab:** Access Control and Permissions
