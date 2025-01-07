from typing import Literal

from databricks.sdk.runtime import *
from pyspark.sql import DataFrame

CONNECTION_STRINGS = {
    "mysql": "jdbc:mysql://{host}:{port}/{database}",
    "sqlserver": "jdbc:sqlserver://{host}:{port};encrypt=false;",
    "postgresql": "jdbc:postgresql://{host}:{port}/{database}",
}


class DatabaseConnector:
    """Class for Database Connector."""

    def __init__(
        self,
        host: str,
        port: str,
        database: str,
        username: str,
        password: str,
        engine: Literal["mysql", "sqlserver", "postgresql"] = None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        # Define db_type based on the database value
        self.engine = (
            (
                "sqlserver"
                if any(
                    term in database.lower() for term in ["db_financeiro_dbo"]
                )
                else "mysql"
            )
            if not engine
            else engine
        )

    def read_table_using_jdbc(
        self, table_name: str = None, query: str = None
    ) -> DataFrame:
        """Reading PySpark Table from Database using JDBC.

        Args:
            table_name (str): Table name.
            query (str): Query to execute.

        Returns:
            DataFrame: PySpark DataFrame of Table.
        """
        print(
            f"[{self.engine.upper()}] > Reading Table From '{self.host}:{self.port}/{self.database}' - Table: '{self.database}/{table_name}'."
        )

        url = CONNECTION_STRINGS[self.engine].format(
            host=self.host, port=self.port, database=self.database
        )

        options = {
            "url": url,
            "isolationLevel": "READ_COMMITTED",
            "user": self.username,
            "password": self.password,
            "inferSchema": "true",
            "header": "true",
        }

        if query:
            options["query"] = query
        else:
            options["dbtable"] = table_name

        df = spark.read.format("jdbc").options(**options).load()

        return df
