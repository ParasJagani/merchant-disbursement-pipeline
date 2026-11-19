# Three roles, one per kind of work:
#   LOADER       stages files and loads bronze (the file writer and the load task)
#   TRANSFORMER  builds silver and gold (dbt, dynamic tables, stream tasks)
#   REPORTER     reads gold (dashboard and PDF statements)
# Object-level grants on stages and tables are in sql/10_bronze.sql, because those objects are
# created by the SQL deployment. Later phases add the transformer and reporter grants.

resource "snowflake_account_role" "loader" {
  name    = "LOADER"
  comment = "File writer and bronze load"
}

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER"
  comment = "dbt, dynamic tables and stream tasks"
}

resource "snowflake_account_role" "reporter" {
  name    = "REPORTER"
  comment = "Dashboard and statements, read-only on gold"
}

# --- warehouse usage -----------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "loader_ingest_wh" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ingest.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_transform_wh" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_transform_wh" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform.name
  }
}

# --- database and schema usage ---------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  for_each = {
    loader      = snowflake_account_role.loader.name
    transformer = snowflake_account_role.transformer.name
    reporter    = snowflake_account_role.reporter.name
  }

  account_role_name = each.value
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_bronze_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.layers["BRONZE"].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_bronze_usage" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.layers["BRONZE"].fully_qualified_name
  }
}
