# Resource monitors first, so each warehouse can reference its monitor on creation.
resource "snowflake_resource_monitor" "ingest" {
  name                      = "${var.ingest_warehouse_name}_MONITOR"
  credit_quota              = var.ingest_monthly_credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [50, 75]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_resource_monitor" "transform" {
  name                      = "${var.transform_warehouse_name}_MONITOR"
  credit_quota              = var.transform_monthly_credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [50, 75]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_warehouse" "ingest" {
  name           = var.ingest_warehouse_name
  warehouse_size = var.warehouse_size
  warehouse_type = "STANDARD"
  comment        = "Loads staged files into bronze"

  # Short bursts only: suspend fast, resume on demand.
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  min_cluster_count = 1
  max_cluster_count = 1
  scaling_policy    = "STANDARD"

  resource_monitor = snowflake_resource_monitor.ingest.name
}

resource "snowflake_warehouse" "transform" {
  name           = var.transform_warehouse_name
  warehouse_size = var.warehouse_size
  warehouse_type = "STANDARD"
  comment        = "Silver and gold refreshes, dbt runs"

  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true

  min_cluster_count = 1
  max_cluster_count = 1
  scaling_policy    = "STANDARD"

  resource_monitor = snowflake_resource_monitor.transform.name
}
