# Databricks notebook source

from data_platform_pipelines.pipelines.utils.conn.aws import SecretsManager
from data_platform_pipelines.pipelines.utils.databases.database_connector import DatabaseConnector
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging


logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


if __name__ == "__main__":
    driver = "org.mariadb.jdbc.Driver"

    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks()

    if widgets['secrets_provider'] == 'aws':
        # Obtendo credenciais do banco de dados a partir do AWS Secrets Manager
        credentials = SecretsManager().get_secrets(secret_name=widgets["db_name"])
        database_host = credentials.get("host")
        database_port = credentials.get("port")
        user = credentials.get("username")
        password = credentials.get("password")

    elif widgets['secrets_provider'] == 'databricks':
        # Obtendo credenciais do banco de dados a partir do Databricks Secrets
        database_host = dbutils.secrets.get(scope=widgets["db_name"], key="host")
        database_port = dbutils.secrets.get(scope=widgets["db_name"], key="port")
        user = dbutils.secrets.get(scope=widgets["db_name"], key="username")
        password = dbutils.secrets.get(scope=widgets["db_name"], key="password")

    # Iterando sobre os esquemas e tabelas para processamento
    for schema, tables in widgets["schemas"].items():
        for table, about_ingestion in tables.items():
            # Montando o nome completo da tabela
            table_name = table if schema in ["db_financeiro_dbo"] else f"{schema}.{table}"

            # Instanciando a classe DatabaseConnector
            connector = DatabaseConnector(
                host=database_host,
                port=database_port,
                database=schema,
                username=user,
                password=password
            )

            # Verificando se a replicação é incremental
            if widgets["incremental"] and about_ingestion['incremental_col'] != "-":
                logging.info(f"Processando {widgets['db_name']}.{schema}.{table} com coluna incremental '{about_ingestion['incremental_col']}'...")

                # Obtendo o timestamp do último COPY INTO executado na Delta Table
                last_update = DataLake.get_max_timestamp_from_delta_table(
                    catalog=widgets["catalog"],
                    schema=schema,
                    table=table,
                    incremental_col=about_ingestion['incremental_col']
                )
                logging.info(f"Última atualização incremental: {last_update}")


                # Construindo a consulta SQL incremental
                query = f"""
                    SELECT
                        *
                    FROM
                        {table_name}
                    WHERE
                        {about_ingestion['incremental_where']} '{last_update}'
                """

                # Lendo a tabela remota usando JDBC
                remote_table = connector.read_table_using_jdbc(
                    query=query
                )

            else:
                try:
                    logging.info(f"Processando {widgets['db_name']}.{schema}.{table} com ingestão {about_ingestion['full_where']}...")

                    query = f"""
                        SELECT
                            *
                        FROM
                            {table_name}
                        WHERE
                            {about_ingestion['full_where']}
                    """

                    remote_table = connector.read_table_using_jdbc(
                        query=query
                    )
                except:
                    logging.info(f"Processando {widgets['db_name']}.{schema}.{table} com ingestão FULL...")

                    # Lendo a tabela remota usando JDBC
                    remote_table = connector.read_table_using_jdbc(
                        table_name=table_name
                    )

            if remote_table.count() == 0:
                if widgets["incremental"]:
                    logging.warning(f"{widgets['db_name']}.{schema}.{table} não possui dados com '{incremental_col}' maior que {last_update}")
                else:
                    logging.warning(f"{widgets['db_name']}.{schema}.{table} não possui dados no banco de origem")

            else:
                # Adicionando campos técnicos
                remote_table_with_metadatas = DataLake.add_metadata(
                    df=remote_table,
                    data_source=widgets["db_name"].capitalize(),
                    source_type=widgets["metadata"]["source_type"],
                    cost_center=widgets["metadata"]["cost_center"],
                    system_owner=widgets["metadata"]["system_owner"]
                )

                # Criando o caminho para salvar os dados no Data Lake
                if "." in table:
                    table = table.split(".")[-1]

                dl_path = widgets["outputs"].format(
                        origin=widgets["db_name"],
                        schema=schema,
                        table=table
                        )

                if about_ingestion['incremental_col'] == "-":
                    DataLake.write_table_as_json(df=remote_table_with_metadatas, dl_path=dl_path, mode="overwrite")
                    logging.info(f"{widgets['db_name']}.{schema}.{table} foi sobrescrito no Data Lake com sucesso em {dl_path}")
                else:
                    DataLake.write_table_as_json(df=remote_table_with_metadatas, dl_path=dl_path)
                    logging.info(f"{widgets['db_name']}.{schema}.{table} foi appendado no Data Lake com sucesso em {dl_path}")