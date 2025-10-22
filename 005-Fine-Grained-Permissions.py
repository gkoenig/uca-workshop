# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Lab: Fine-Grained Security
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand row-level and column-level security concepts
# MAGIC - Implement data masking for sensitive information
# MAGIC - Leverage attribute based access control for dynamic security at scale
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 1-4
# MAGIC - Access to tables created in previous labs

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

spark.sql(f"SELECT * FROM `{catalog_name}`.`{schema_name}`.`customers`").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalog_name}`.`{schema_name}`.`suppliers`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Applying Fine-Grained Security in Unity Catalog
# MAGIC
# MAGIC Unity Catalog implements a comprehensive security model through security functions, attribute-based access control (ABAC), and governed tags. These components work together to provide granular data protection.
# MAGIC
# MAGIC **Security Implementation Components**:
# MAGIC - **Security Functions**: SQL functions that define access logic and data filtering rules
# MAGIC - **ABAC Policies**: Attribute-based policies that evaluate user context and data attributes

# COMMAND ----------

# DBTITLE 1,Create Row Filter Function
spark.sql(f"""
          CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.filter_nation(nation string)
          RETURN IF(IS_ACCOUNT_GROUP_MEMBER('geo'), true, nation IN ("UNITED STATES", "CANADA"))
""")

# COMMAND ----------

# DBTITLE 1,Create Column Masking Function
spark.sql(f"""
          CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.mask_address(address string)
          RETURN IF(IS_ACCOUNT_GROUP_MEMBER('hr_admin'), address, "XXX Masked Address")
""")

# COMMAND ----------

# DBTITLE 1,Create ABAC Policy: Row Filtering
spark.sql(f"""CREATE POLICY nation_filter
              ON SCHEMA {catalog_name}.{schema_name}
              COMMENT 'Filter out nations for non-HR users'
              ROW FILTER {catalog_name}.{schema_name}.filter_nation
              TO `account users`
              FOR TABLES
              MATCH COLUMNS
                hasTag('uc_geo') AS nation
              USING COLUMNS (nation)
""")

# COMMAND ----------

# DBTITLE 1,Create ABAC Policy: Column Masking
spark.sql(f"""CREATE POLICY mask_address
              ON SCHEMA {catalog_name}.{schema_name}
              COMMENT 'Mask PII address information.'
              COLUMN MASK {catalog_name}.{schema_name}.mask_address
              TO `account users`
              FOR TABLES
              MATCH COLUMNS
                hasTagValue('uc_pii', 'address') AS address
              ON COLUMN address
""")

# COMMAND ----------

# DBTITLE 1,Apply Governed Tags
spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.customers ALTER COLUMN c_address SET TAGS ('uc_pii' = 'address')")

spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.suppliers SET TAGS ('uc_geo')")
spark.sql(f"ALTER TABLE {catalog_name}.{schema_name}.suppliers ALTER COLUMN n_name SET TAGS ('uc_geo')")

# COMMAND ----------

# DBTITLE 1,Test Applied Permissions: Customers
spark.sql(f"SELECT * FROM `{catalog_name}`.`{schema_name}`.`customers`").display()

# COMMAND ----------

# DBTITLE 1,Test Applied Permissions: Suppliers
spark.sql(f"SELECT * FROM `{catalog_name}`.`{schema_name}`.`suppliers`").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC
# MAGIC **What you learned:**
# MAGIC - ✅ **Unity Catalog Security Functions**: Building reusable functions registered in Unity Catalog
# MAGIC - ✅ **ABAC (Attribute-Based Access Control)**: Policy-driven security using user attributes, groups, and context
# MAGIC - ✅ **Governed Tags**: Metadata-driven classification and automatic policy enforcement
# MAGIC
# MAGIC
# MAGIC **Key Commands Used:**
# MAGIC - `CREATE FUNCTION <name> ...`
# MAGIC - `ALTER TABLE <table> SET ROW FILTER <function> ON (<column>)`
# MAGIC - `ALTER TABLE <table> ALTER COLUMN <column> SET MASK <function>`
# MAGIC - `ALTER TABLE <table> SET TAGS ('<tag_name>' = '<value>')`
# MAGIC - `SELECT current_user(), current_groups()`
# MAGIC - `SELECT is_member('<group_name>')`
# MAGIC
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Use governed tags for consistent data classification
# MAGIC - Implement ABAC policies for scalable security management
# MAGIC - Test security functions with different user contexts
# MAGIC - Document tag taxonomy and policy mappings
# MAGIC - Combine multiple security layers (row, column, object-level)
# MAGIC - Regular review and audit of applied policies
# MAGIC
# MAGIC
# MAGIC **Troubleshooting Tips:**
# MAGIC - Verify user group memberships with `current_groups()`
# MAGIC - Test security functions in different execution contexts
# MAGIC - Validate filter/mask expressions before applying to production tables
# MAGIC
