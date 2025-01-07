# Databricks notebook source

from databricks import sql
import os


def start():
    """
    Prints a message indicating that the Databricks cluster is up.
    """
    databricks_token = dbutils.secrets.get(scope="databricks", key="token")

    with sql.connect(server_hostname = "dbc-aa960f0c-f7ef.cloud.databricks.com",
                    http_path       = "/sql/1.0/warehouses/b5ac507cf171aff5",
                    access_token    = databricks_token) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchall()

    print("Databricks DW Cluster UP!")

    return 1

if __name__ == "__main__":
    start()