# Databricks notebook source

import logging
from datetime import datetime

from pyspark.sql import DataFrame

from data_platform_pipelines.pipelines.utils.conn.aws import SecretsManager
from data_platform_pipelines.pipelines.utils.databases.database_connector import (
    DatabaseConnector,
)
from data_platform_pipelines.pipelines.utils.datalake import DataLake

# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def collect_data(
    widgets: dict,
    schema_name: str,
    table_name: str,
    about_ingestion: dict,
    **credentials,
):
    # Instanciando a classe DatabaseConnector
    connector = DatabaseConnector(
        host=credentials["host"],
        port=credentials["port"],
        database=widgets["db_name"],
        username=credentials["username"],
        password=credentials["password"],
        engine=widgets["engine"],
    )

    # Verificando se a replicação é incremental
    if widgets["incremental"] and about_ingestion["incremental_col"] != "-":
        logging.info(
            f"Processando {widgets['source_type']}.{widgets['data_source']}.{schema_name}.{table_name} com coluna incremental '{about_ingestion['incremental_col']}'..."
        )

        # Obtendo o timestamp do último COPY INTO executado na Delta Table
        last_update = DataLake.get_max_timestamp_from_delta_table(
            catalog=widgets["catalog"],
            schema=schema_name
            if widgets["data_source"] in schema_name
            else f"{widgets['data_source']}_{schema_name}",
            table=table_name,
            incremental_col=about_ingestion["incremental_col"],
        )
        logging.info(f"Última atualização incremental: {last_update}")

        # Construindo a consulta SQL incremental
        query = f"""
            SELECT
                *
            FROM
                {schema_name}.{table_name}
            WHERE
                {about_ingestion["incremental_where"]} '{last_update}'
        """

        # Lendo a tabela remota usando JDBC
        df = connector.read_table_using_jdbc(query=query)

    else:
        try:
            logging.info(
                f"Processando {schema_name}.{table_name} com ingestão {about_ingestion['full_where']}..."
            )

            query = f"""
                SELECT
                    *
                FROM
                    {schema_name}.{table_name}
                WHERE
                    {about_ingestion["full_where"]}
            """

            df = connector.read_table_using_jdbc(query=query)
        except:
            logging.info(
                f"Processando {schema_name}.{table_name} com ingestão FULL..."
            )

            # Lendo a tabela remota usando JDBC
            df = connector.read_table_using_jdbc(table_name=table_name)

    # Checa se há dados existentes para salvar no Data Lake
    if df.count() == 0:
        if widgets["incremental"]:
            logging.warning(
                f"{schema_name}.{table_name} não possui dados com '{about_ingestion['incremental_col']}' maior que {last_update}"
            )
        else:
            logging.warning(
                f"{schema_name}.{table_name} não possui dados no banco de origem"
            )
        return

    return df


def save_to_landing(
    df: DataFrame, widgets: dict, schema_name: str, table_name: str, mode: str
):
    # Adiciona campos técnicos ao dataframe
    metadatas = {
        "_data_source": widgets["metadata"]["data_source"],
        "_source_type": widgets["metadata"]["source_type"],
        "_cost_center": widgets["metadata"]["cost_center"],
        "_system_owner": widgets["metadata"]["system_owner"],
    }
    df_with_metadatas = DataLake.add_metadata_v2(df, metadatas)

    # Cria o caminho para salvar os dados no Data Lake
    dl_path = widgets["outputs"].format(
        source_type=widgets["source_type"],
        data_source=widgets["data_source"],
        schema=schema_name,
        table=table_name,
    )

    # Salva os dados como csv no Data Lake
    DataLake.write_table_as_json(
        df=df_with_metadatas, dl_path=dl_path, mode=mode
    )
    logging.info(
        f"{table_name} foi escrito como '{mode}' no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Five9 a partir do AWS Secrets Manager
    credentials = SecretsManager().get_secrets(
        secret_name=widgets["data_source"]
    )

    # Iterando sobre os esquemas e tabelas para processamento
    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            df = collect_data(
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
                about_ingestion=about_ingestion,
                **credentials,
            )
            if df:
                save_to_landing(
                    df=df,
                    widgets=widgets,
                    schema_name=schema_name,
                    table_name=table_name,
                    mode="append"
                    if about_ingestion["incremental_col"] != "-"
                    else "overwrite",
                )


if __name__ == "__main__":
    main()
