terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

data "databricks_cluster" "existing" {
  cluster_name = "Development Cluster"
}

resource "databricks_permissions" "admin_access" {
  cluster_id = data.databricks_cluster.existing.id


  access_control {
    user_name = "leandro.frazao@alloha.com"
    permission_level = "CAN_MANAGE"
  }

}

resource "databricks_permissions" "full_access" {
  cluster_id = data.databricks_cluster.existing.id

  access_control {
    group_name = "G_Alloha_Analytics"
    permission_level = "CAN_RESTART"
  }

  access_control {
    group_name = "G_Alloha_Engineers"
    permission_level = "CAN_RESTART"
  }

  access_control {
    group_name = "G_Engineers"
    permission_level = "CAN_MANAGE"
  }

}