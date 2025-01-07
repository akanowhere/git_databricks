terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

resource "databricks_permissions" "admin_access" {
  sql_endpoint_id = var.id


  access_control {
    user_name = "leandro.frazao@alloha.com"
    permission_level = "CAN_MANAGE"
  }

}

resource "databricks_permissions" "full_access" {
  sql_endpoint_id = var.id

  access_control {
    group_name       = "G_Alloha_Analytics"
    permission_level = "CAN_USE"
  }

  access_control {
    group_name       = "G_Alloha_Engineers"
    permission_level = "CAN_USE"
  }

  access_control {
    group_name       = "G_Alloha_B2C"
    permission_level = "CAN_USE"
  }

  access_control {
    group_name       = "G_Engineers"
    permission_level = "CAN_MANAGE"
  }

}