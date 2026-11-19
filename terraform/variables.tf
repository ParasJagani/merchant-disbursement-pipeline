variable "snowflake_organization_name" {
  description = "Snowflake organization name (first part of the account identifier)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name within the organization."
  type        = string
}

variable "snowflake_user" {
  description = "Service user Terraform authenticates as (key-pair auth)."
  type        = string
  default     = "TF_SERVICE_USER"
}

variable "snowflake_private_key_path" {
  description = "Path to the private key file of the Terraform service user."
  type        = string
}

variable "snowflake_bootstrap_role" {
  description = "Role Terraform runs as. ACCOUNTADMIN on the trial; narrow it on a real account."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "database_name" {
  description = "Database this project owns."
  type        = string
  default     = "MERCHANT_DISBURSEMENT"
}

variable "ingest_warehouse_name" {
  type    = string
  default = "INGEST_WH"
}

variable "transform_warehouse_name" {
  type    = string
  default = "TRANSFORM_WH"
}

variable "warehouse_size" {
  description = "Kept at XSMALL on purpose: this volume does not need more."
  type        = string
  default     = "XSMALL"
}

variable "ingest_monthly_credit_quota" {
  description = "Resource monitor quota for the ingest warehouse."
  type        = number
  default     = 15
}

variable "transform_monthly_credit_quota" {
  description = "Resource monitor quota for the transform warehouse (dynamic tables run here)."
  type        = number
  default     = 40
}
