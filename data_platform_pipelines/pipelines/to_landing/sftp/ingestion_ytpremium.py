# Databricks notebook source

%pip install paramiko==3.5.0  # type: ignore
%pip install chardet  # type: ignore

import logging
from datetime import datetime
from typing import Union

from pyspark.sql import DataFrame

from data_platform_pipelines.pipelines.utils.conn.aws import SecretsManager
from data_platform_pipelines.pipelines.utils.datalake import DataLake
from data_platform_pipelines.pipelines.utils.sftp.ytpremium_collector import (
    YTPremiumCollector,
)

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
) -> DataFrame:
    # Inicializando o objeto coletor da classe InfobipCollector
    collector = YTPremiumCollector(**credentials)

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

        n_days = (
            datetime.now() - datetime.strptime(last_update, "%Y-%m-%d")
        ).days - 1
    else:
        logging.info(
            f"Processando {schema_name}.{table_name} de forma full..."
        )
        n_days = None

    df = collector.start(
        prefix=about_ingestion["prefix"],
        specific_path=about_ingestion["path"],
        n_days=n_days,
        incremental=widgets["incremental"],
    )

    return df


def save_to_landing(
    df: Union[DataFrame, None],
    widgets: dict,
    schema_name: str,
    table_name: str,
):
    # Checa se há dados existentes para salvar no Data Lake
    if df is None or df.count() == 0:
        logging.warning(
            f"{table_name} não possui dados disponíveis para o período especificado.."
        )
        return

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

    # Salva os dados como json no Data Lake
    DataLake.write_table_as_json(
        df=df_with_metadatas.repartition(1), dl_path=dl_path
    )
    logging.info(
        f"{table_name} foi appendado no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Five9 a partir do AWS Secrets Manager
    credentials = SecretsManager().get_secrets(secret_name="youtube_premium")

    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            df = collect_data(
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
                about_ingestion=about_ingestion,
                **credentials,
            )
            save_to_landing(
                df=df,
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
            )


if __name__ == "__main__":
    main()
