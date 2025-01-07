module "cluster" {
    source = "./cluster"
    host = var.databricks_host
    token = var.databricks_token
}

module "sql_warehouse" {
    source = "./sql_warehouse"
    host = var.databricks_host
    token = var.databricks_token
    id = var.sql_endpoint_id
}