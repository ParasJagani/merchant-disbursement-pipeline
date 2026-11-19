resource "snowflake_database" "this" {
  name    = var.database_name
  comment = "Merchant disbursement pipeline"

  # One day of Time Travel is enough here and keeps storage cost down.
  data_retention_time_in_days = 1
}

locals {
  schemas = {
    BRONZE     = "Raw messages, one append-only table per topic"
    SILVER     = "Current-state tables: stream-built reference data and dynamic-table events"
    GOLD       = "Ledger and daily disbursement"
    QUARANTINE = "Rows rejected by validation, with a reason"
    OPS        = "Tasks, procedures and monitoring tables"
  }
}

resource "snowflake_schema" "layers" {
  for_each = local.schemas

  database = snowflake_database.this.name
  name     = each.key
  comment  = each.value
}
