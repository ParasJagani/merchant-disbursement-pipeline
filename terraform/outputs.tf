output "database_name" {
  value = snowflake_database.this.name
}

output "ingest_warehouse" {
  value = snowflake_warehouse.ingest.name
}

output "transform_warehouse" {
  value = snowflake_warehouse.transform.name
}

output "roles" {
  value = {
    loader      = snowflake_account_role.loader.name
    transformer = snowflake_account_role.transformer.name
    reporter    = snowflake_account_role.reporter.name
  }
}
