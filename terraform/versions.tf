terraform {
  required_version = ">= 1.5"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.19"
    }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(pathexpand(var.snowflake_private_key_path))

  # Trial simplification: ACCOUNTADMIN is needed to create resource monitors, roles and grants.
  # Day-to-day runs use the LOADER, TRANSFORMER and REPORTER roles created below.
  role = var.snowflake_bootstrap_role
}
