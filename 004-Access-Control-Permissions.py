# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Lab: Access Control and Permissions
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand Unity Catalog's permission model
# MAGIC - Grant and revoke permissions at each level in the 
# MAGIC - Test access control with different users/groups
# MAGIC - Use SHOW GRANTS for permission auditing
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed Labs 1, 2 & 3

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
# MAGIC ## Step 2: Granting and Viewing Permissions
# MAGIC
# MAGIC Unity Catalog uses a hierarchical permission model. Permissions can be inherited from parent objects.
# MAGIC
# MAGIC **Permission Hierarchy**:
# MAGIC - **USE_CATALOG**: Access to catalog objects
# MAGIC - **USE_SCHEMA**: Access to schema objects  
# MAGIC - **CREATE_TABLE**: Ability to create tables in schema
# MAGIC - **CREATE_VOLUME**: Ability to create volumes
# MAGIC - **READ_VOLUME/WRITE_VOLUME**: File access permissions
# MAGIC
# MAGIC **Best Practice**: Grant minimal required permissions and use groups instead of individual users.
# MAGIC
# MAGIC Let's start by examining what permissions exist on our objects.

# COMMAND ----------

# DBTITLE 1,Grant catalog-level permissions
spark.sql(f"GRANT USE_CATALOG ON CATALOG `{catalog_name}` TO `{user_name}`;")
spark.sql(f"GRANT BROWSE ON CATALOG `{catalog_name}` TO `{user_name}`;")

# COMMAND ----------

# DBTITLE 1,Grant schema-level permissions
spark.sql(f"GRANT USE_SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` TO `account users`;")

# COMMAND ----------

# DBTITLE 1,Grant table creation and select permissions
spark.sql(f"GRANT SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` TO `account users`;")
spark.sql(f"GRANT SELECT ON TABLE `{catalog_name}`.`{schema_name}`.sales_fact TO `account users`;")

# COMMAND ----------

# DBTITLE 1,Grant volume creation and access permissions
spark.sql(f"GRANT READ_VOLUME ON VOLUME `{catalog_name}`.`{schema_name}`.tracking TO `account users`;")

# COMMAND ----------

# DBTITLE 1,Display Catalog Permissions
spark.sql(f"SHOW GRANTS ON CATALOG {catalog_name}").display()

# COMMAND ----------

# DBTITLE 1,Display Schema Permissions
spark.sql(f"SHOW GRANTS ON SCHEMA {schema_name}").display()

# COMMAND ----------

# DBTITLE 1,Display Table Permissions
spark.sql(f"SHOW GRANTS ON TABLE {catalog_name}.{schema_name}.sales_fact").display()

# COMMAND ----------

# DBTITLE 1,Display Volume Permissions
spark.sql(f"SHOW GRANTS ON VOLUME {catalog_name}.{schema_name}.tracking").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Revoking Permissions
# MAGIC
# MAGIC Let's learn how to revoke permissions when they're no longer needed.

# COMMAND ----------

# DBTITLE 1,Revoke Table Level Permissions
spark.sql(f"REVOKE READ_VOLUME ON VOLUME `{catalog_name}`.`{schema_name}`.tracking FROM `account users`")
spark.sql(f"REVOKE SELECT ON TABLE `{catalog_name}`.`{schema_name}`.sales_fact FROM `account users`")

# COMMAND ----------

# DBTITLE 1,Revoke Schema Level Permissions
spark.sql(f"REVOKE SELECT ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `account users`")
spark.sql(f"REVOKE USE_SCHEMA ON SCHEMA `{catalog_name}`.`{schema_name}` FROM `account users`")

# COMMAND ----------

# DBTITLE 1,Revoke Catalog Level Permissions
spark.sql(f"REVOKE BROWSE ON CATALOG `{catalog_name}` FROM `account users`") 
spark.sql(f"REVOKE USE_CATALOG ON CATALOG `{catalog_name}` FROM `account users`") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Permission Auditing
# MAGIC
# MAGIC Use information schema to programmatically query permissions.

# COMMAND ----------

# DBTITLE 1,Query table privileges from information schema
spark.sql(f"""SELECT 
  table_catalog,
  table_schema,
  table_name,
  privilege_type,
  inherited_from,
  grantee
FROM system.information_schema.table_privileges 
WHERE table_catalog = '{catalog_name}'
AND table_schema = '{schema_name}'
ORDER BY table_schema, table_name, privilege_type
""").display()

# COMMAND ----------

# DBTITLE 1,Query schema privileges
spark.sql(f"""SELECT 
  catalog_name,
  schema_name,
  privilege_type,
  inherited_from,
  grantee
FROM information_schema.schema_privileges 
WHERE catalog_name = '{catalog_name}'
AND schema_name = '{schema_name}'
ORDER BY schema_name, privilege_type
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC **What you learned:**
# MAGIC - ✅ **Permission Model**: Hierarchical structure (Metastore → Catalog → Schema → Object)
# MAGIC - ✅ **Key Permissions**: 
# MAGIC   - `USE CATALOG`, `USE SCHEMA` - Required for access
# MAGIC   - `SELECT`, `READ FILES` - Data operations
# MAGIC   - `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE` - Object creation
# MAGIC - ✅ **Inheritance**: Permissions can be inherited from parent objects, or granted at each individual level
# MAGIC - ✅ **Auditing**: Use `SHOW GRANTS` and `information_schema` for monitoring
# MAGIC
# MAGIC **Key Commands Used:**
# MAGIC - `GRANT <permission> ON <object> TO <principal>`
# MAGIC - `REVOKE <permission> ON <object> FROM <principal>` 
# MAGIC - `SHOW GRANTS ON <object>`
# MAGIC - `information_schema.table_privileges`
# MAGIC - `information_schema.schema_privileges`
# MAGIC
# MAGIC **Best Practices:**
# MAGIC - Regularly audit permissions
# MAGIC - Follow principle of least privilege
# MAGIC - Document permission policies for your organization
# MAGIC - Use groups instead of individual user grants
# MAGIC
# MAGIC **Troubleshooting Tips:**
# MAGIC - Check all prerequisite permissions for operations
# MAGIC - Verify object existence before granting permissions  
# MAGIC - Use information schema for systematic permission reviews
# MAGIC
# MAGIC **Next Lab:** Fine-Grained Security
