from datetime import datetime

from databricks.sdk.runtime import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class DataLake:
    """Classe para camada de Data Lake."""

    @staticmethod
    def write_table_as_json(
        df: DataFrame, dl_path: str, mode: str = "append"
    ) -> None:
        """
        Escreve a tabela como CSV na camada do Data Lake.

        Args:
            df (DataFrame): DataFrame a ser escrito.
            dl_path (str): Caminho do Data Lake no S3.
            mode (str): Modo de escrita, opções: 'overwrite', 'append', etc.
        """
        (
            df.write.option("header", True)
            .option("inferSchema", True)
            .mode(mode)
            .json(dl_path)
        )

    @staticmethod
    def write_table_as_parquet(
        df: DataFrame, dl_path: str, mode: str = "append"
    ) -> None:
        """
        Escreve a tabela como CSV na camada do Data Lake.

        Args:
            df (DataFrame): DataFrame a ser escrito.
            dl_path (str): Caminho do Data Lake no S3.
            mode (str): Modo de escrita, opções: 'overwrite', 'append', etc.
        """
        df.write.mode(mode).parquet(dl_path)

    @staticmethod
    def write_table_as_csv(
        df: DataFrame, dl_path: str, mode: str = "append"
    ) -> None:
        """
        Escreve a tabela como CSV na camada do Data Lake.

        Args:
            df (DataFrame): DataFrame a ser escrito.
            dl_path (str): Caminho do Data Lake no S3.
            mode (str): Modo de escrita, opções: 'overwrite', 'append', etc.
        """
        (
            df.coalesce(1)
            .write.option("header", True)
            .option("inferSchema", True)
            .mode(mode)
            .csv(dl_path)
        )

    @staticmethod
    def get_max_timestamp_from_delta_table(
        catalog: str, schema: str, table: str, incremental_col: str
    ) -> datetime:
        """
        Obtém o timestamp máximo da tabela Delta.

        Args:
            catalog (str): Catálogo Delta.
            schema (str): Esquema da tabela.
            table (str): Nome da tabela.
            operation (str): Operação Delta, padrão é "COPY INTO".

        Returns:
            datetime: Timestamp máximo.
        """
        query = f"""
            SELECT
                MAX({incremental_col})
            FROM
                {catalog}.{schema}.{table}
            WHERE
                {incremental_col} <= NOW();
        """

        return spark.sql(query).collect()[0][0]

    @staticmethod
    def get_landing_widgets_from_databricks() -> dict:
        """
        Obtém os widgets do Databricks.

        Returns:
            dict: Dicionário contendo os widgets.
        """
        # Handling for when this is not set
        try:
            secrets_provider = str(dbutils.widgets.get("secrets_provider"))
        except:
            secrets_provider = "databricks"
            
        return {
            "catalog": str(dbutils.widgets.get("catalog")),
            "metadata": eval(dbutils.widgets.get("metadata")),
            "schemas": eval(dbutils.widgets.get("schemas")),
            "outputs": str(dbutils.widgets.get("outputs")),
            "incremental": eval(dbutils.widgets.get("incremental")),
            "db_name": str(dbutils.widgets.get("db_name")),
            "secrets_provider": secrets_provider
        }

    @staticmethod
    def get_landing_widgets_from_databricks_v2() -> dict:
        """
        Obtém os widgets do Databricks.

        Returns:
            dict: Dicionário contendo os widgets.
        """
        try:
            return {
                "catalog": str(dbutils.widgets.get("catalog")),
                "metadata": eval(str(dbutils.widgets.get("metadata"))),
                "schemas": eval(str(dbutils.widgets.get("schemas"))),
                "outputs": str(dbutils.widgets.get("outputs")),
                "incremental": eval(dbutils.widgets.get("incremental")),
                "source_type": str(dbutils.widgets.get("source_type")),
                "data_source": str(dbutils.widgets.get("data_source")),
                "frequency": str(dbutils.widgets.get("frequency")),
                "db_name": str(dbutils.widgets.get("db_name")),
                "engine": str(dbutils.widgets.get("engine")),
            }
        except:
            return {
                "catalog": str(dbutils.widgets.get("catalog")),
                "metadata": eval(str(dbutils.widgets.get("metadata"))),
                "schemas": eval(str(dbutils.widgets.get("schemas"))),
                "outputs": str(dbutils.widgets.get("outputs")),
                "incremental": eval(dbutils.widgets.get("incremental")),
                "source_type": str(dbutils.widgets.get("source_type")),
                "data_source": str(dbutils.widgets.get("data_source")),
                "frequency": str(dbutils.widgets.get("frequency")),
            }

    def get_bronze_widgets_from_databricks() -> dict:
        """
        Obtém os widgets do Databricks.

        Returns:
            dict: Dicionário contendo os widgets.
        """
        return {
            "input_path": str(dbutils.widgets.get("input_path")),
            "schemas": eval(dbutils.widgets.get("schemas")),
            "outputs": eval(dbutils.widgets.get("outputs")),
            "db_name": str(dbutils.widgets.get("db_name")),
        }

    def get_bronze_widgets_from_databricks_v2() -> dict:
        """
        Obtém os widgets do Databricks.

        Returns:
            dict: Dicionário contendo os widgets.
        """
        return {
            "input_path": str(dbutils.widgets.get("input_path")),
            "schemas": eval(dbutils.widgets.get("schemas")),
            "outputs": eval(dbutils.widgets.get("outputs")),
            "source_type": str(dbutils.widgets.get("source_type")),
            "data_source": str(dbutils.widgets.get("data_source")),
        }

    def get_cleanup_widgets_from_databricks() -> dict:
        """
        Obtém os widgets do Databricks.

        Returns:
            dict: Dicionário contendo os widgets.
        """
        return {
            "retention_hours": str(dbutils.widgets.get("retention_hours")),
            "catalogs": eval(dbutils.widgets.get("catalogs")),
        }


    @staticmethod
    def add_metadata(
        df: DataFrame,
        data_source: str,
        source_type: str,
        cost_center: str,
        system_owner: str,
    ) -> DataFrame:
        """
        Adiciona metadados de negócios ao DataFrame Spark.

        Args:
            df (DataFrame): O DataFrame Spark ao qual os metadados serão adicionados.
            data_source (str): A fonte dos dados.
            source_type (str): O tipo de fonte dos dados.
            cost_center (str): O centro de custo associado aos dados.
            system_owner (str): O proprietário do sistema responsável pelos dados.

        Returns:
            DataFrame: O DataFrame Spark com as colunas de metadados adicionados.
        """
        return (
            df.withColumn("_data_source", lit(data_source))
            .withColumn("_source_type", lit(source_type))
            .withColumn("_cost_center", lit(cost_center))
            .withColumn("_system_owner", lit(system_owner))
            .withColumn("_qt_rows", lit(df.count()))
        )

    @staticmethod
    def add_metadata_v2(df: DataFrame, metadatas: dict) -> DataFrame:
        """
        Adiciona metadados de negócios ao DataFrame Spark.

        Args:
            df (DataFrame): O DataFrame Spark ao qual os metadados serão adicionados.
            metadatas (dict): Um dicionário contendo os nomes das colunas e seus valores.

        Returns:
            DataFrame: O DataFrame Spark com as colunas de metadados adicionados.
        """
        for column_name, value in metadatas.items():
            df = df.withColumn(column_name, lit(value))

        df = df.withColumn("_qtd_rows", lit(df.count()))

        return df

    @staticmethod
    def copy_data_to_delta_table(
        catalog: str,
        schema: str,
        table: str,
        target_table: str,
        source: str,
        file_format: str = "JSON",
        mode: str = "append",
    ) -> None:
        """
        Função para copiar dados para uma tabela delta em um catálogo usando Spark SQL.

        Args:
            catalog (str): O nome do catálogo.
            schema (str): O nome do esquema.
            table (str): O nome da tabela.
            target_table (str): O caminho de armazenamento dos dados.
            source (str): O diretório de saída dos dados a serem copiados.
            run (str): O nome da pasta com a data da última execução.
        """
        use_catalog = f"USE CATALOG {catalog}"

        create_schema = f"""
        CREATE SCHEMA IF NOT EXISTS {schema}
        COMMENT 'A new Unity Catalog schema called {schema}'
        """

        create_table = f"""
                        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table}
                        LOCATION '{target_table}';
                    """

        drop_table = f"""
                        DROP TABLE IF EXISTS {catalog}.{schema}.{table};
                    """

        copy_into = f"""
                    COPY INTO {catalog}.{schema}.{table}
                    FROM (
                        SELECT *, _metadata
                        FROM '{source}'
                    )
                    FILEFORMAT = {file_format}
                    FORMAT_OPTIONS ("header" = "true", "mergeSchema" = "true")
                    COPY_OPTIONS ("header" = "true", "mergeSchema" = "true");
                """

        # Executar comandos Spark SQL
        spark.sql(use_catalog)
        spark.sql(create_schema)
        spark.sql(create_table)

        if mode == "overwrite":
            df = spark.read.json(source).select("*", "_metadata")
            df = df.coalesce(1)

            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("path", target_table)
                .saveAsTable(f"{catalog}.{schema}.{table}")
            )

        else:
            spark.sql(copy_into)
            
    
    @staticmethod
    def optimize_table(
        table_path: str
    ) -> tuple[int, int, int]:
        """
        Aplica a operação de OPTIMIZE na tabela delta.

        Args:
            table_path: o caminho da tabela delta no formato catalog.schema.table
        """
        print(
            f"Optimize on Table: '{table_path}' - "
        )

        optimize = f"""  
            OPTIMIZE {table_path}
        """
        
        results = spark.sql(optimize)
        
        query_results = results.select("metrics.numFilesAdded", "metrics.numFilesRemoved",  "metrics.filesRemoved.totalSize").collect()[0]
        num_files_added = query_results['numFilesAdded']
        num_files_removed = query_results['numFilesRemoved']
        total_size = query_results['totalSize']
        
        return num_files_added, num_files_removed, total_size

    @staticmethod
    def vacuum_table(
        table_path: str,
        retention_hours: int = 168,
    ) -> int:
        """
        Aplica a operação de VACUUM na tabela delta.

        Args:
            table_path (str): o caminho da tabela delta no formato catalog.schema.table
            retention_hours (int): A quantidade de horas a serem mantidas no vacuum.
        """
        print(
            f"Vacuum on Table: '{table_path}' - "
            f"Retain Hours: '{retention_hours}h'."
        )
        vacuum_dry = f"""  
            VACUUM {table_path} RETAIN {retention_hours} HOURS DRY RUN
        """
        
        vacuum = f"""  
            VACUUM {table_path} RETAIN {retention_hours} HOURS
        """
        
        results = spark.sql(vacuum_dry)
        spark.sql(vacuum)
        
        query_results = results.select("path").collect()
        
        if len(query_results) == 1:
            return 0
        else:
            numFiles = len(query_results)
            return numFiles