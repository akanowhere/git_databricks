variable "databricks_host" {
  description = "Host URL for the Databricks workspace"
  type        = string
}

variable "databricks_token" {
  description = "Token for authenticating with Databricks"
  type        = string
}

variable "sql_endpoint_id" {
  description = "ID of the Databricks SQL Warehouse"
  type        = string
}