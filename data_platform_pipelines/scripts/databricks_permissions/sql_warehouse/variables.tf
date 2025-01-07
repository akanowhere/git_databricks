variable "host" {
  description = "Host URL for the Databricks workspace"
  type        = string
}

variable "token" {
  description = "Token for authenticating with Databricks"
  type        = string
}

variable "id" {
  description = "ID of the Databricks SQL Warehouse"
  type        = string
}