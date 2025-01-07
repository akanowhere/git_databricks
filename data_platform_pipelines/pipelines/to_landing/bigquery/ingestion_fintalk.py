# Databricks notebook source

%pip install google-auth==2.34.0  # type: ignore
%pip install google-cloud-bigquery==3.25.0  # type: ignore


import logging
from datetime import datetime, timedelta
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from data_platform_pipelines.pipelines.utils.bigquery.collector import (
    BigQueryCollector,
)
from data_platform_pipelines.pipelines.utils.conn.aws import SecretsManager
from data_platform_pipelines.pipelines.utils.datalake import DataLake

# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def convert_to_strings(data: list) -> list:
    return [{k: str(v) for k, v in d.items()} for d in data]


def generate_time_intervals(last_update: Union[str, datetime]) -> list:
    """Gera uma lista de intervalos de dias a partir do dia seguinte ao last_update até o dia anterior ao dia atual."""
    intervals = []

    # Realiza um pequeno tratamento em 'last_update' caso for do tipo str
    if isinstance(last_update, str):
        last_update = datetime.strptime(
            last_update.split(".")[0], "%Y-%m-%d %H:%M:%S"
        )

    # Definir o dia atual
    current_date = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Começar a partir do dia seguinte ao last_update
    current_start = (last_update + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Se o last_update for no mesmo dia do current_date, não há intervalos a gerar
    if current_start >= current_date:
        logging.warning(
            f"Nenhum intervalo a ser gerado. O last_update '{last_update.date()}' está no mesmo dia ou é posterior ao dia atual '{current_date}'."
        )
        return intervals

    while current_start < current_date:
        current_end = current_start + timedelta(days=1)

        # Adicionar o intervalo no formato (start_date, end_date)
        intervals.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d"),
        ))

        # Atualizar current_start para o próximo dia (igual ao current_end)
        current_start = current_end

    return intervals


def collect_and_transform_data(
    credentials: dict, query: str, table_name: str
) -> DataFrame:
    """Coleta dados do BigQuery e transforma em um Spark DataFrame."""

    # Coletando os dados do BigQuery
    data = BigQueryCollector(
        service_account_info=credentials, query=query
    ).start()

    # Transformando em um Spark DataFrame
    if "customers" in table_name:
        data = convert_to_strings(data)
        schema = StructType([
            StructField(col, StringType(), True)
            for col in list(data[0].keys())
        ])
        return spark.createDataFrame(data, schema=schema)  # type: ignore

    return spark.createDataFrame(data)  # type: ignore


def collect_data(
    credentials: dict,
    widgets: dict,
    schema_name: str,
    table_name: str,
    about_ingestion: dict,
) -> DataFrame:
    # Verificando se a extração é incremental
    if widgets["incremental"] and about_ingestion["incremental_col"] != "-":
        logging.info(
            f"Processando {widgets['source_type']}.{widgets['data_source']}.{schema_name}.{table_name} com coluna incremental '{about_ingestion['incremental_col']}'..."
        )

        # Obtendo o timestamp do último COPY INTO executado na Delta Table
        last_update = DataLake.get_max_timestamp_from_delta_table(
            catalog=widgets["catalog"],
            schema=schema_name,
            table=table_name,
            incremental_col=about_ingestion["incremental_col"],
        )
        logging.info(f"Última atualização incremental: {last_update}")

        if widgets["frequency"] == "hourly":
            # Construindo a consulta SQL incremental
            query = f"""
                SELECT
                    *
                FROM
                    fintalk-datalake-prd.alloha.vw_{table_name}
                WHERE
                    {about_ingestion["incremental_col"]} > "{last_update}"
            """

            return collect_and_transform_data(credentials, query, table_name)
        else:
            # Obtendo os intervalos start_date e end_date
            intervals = generate_time_intervals(last_update)

            # Se não houver intervalos, interrompa a execução
            if not intervals:
                logging.warning(
                    "Nenhum intervalo encontrado para processar. Parando a execução."
                )
                return None

            # Inicializa a coleta de dados para cada intervalo de start_date e end_date
            df_list = []
            for start_date, end_date in intervals:
                logging.info(f"Processando de {start_date} até {end_date}...")

                query = f"""
                    SELECT
                        *
                    FROM
                        fintalk-datalake-prd.alloha.vw_{table_name}
                    WHERE
                        CAST({about_ingestion["incremental_col"]} AS DATE) >= '{start_date}' 
                        AND CAST({about_ingestion["incremental_col"]} AS DATE) < '{end_date}'
                """

                df_list.append(
                    collect_and_transform_data(credentials, query, table_name)
                )

            # Realizando o union dos DataFrames coletados
            if df_list:
                df = df_list[0]
                for df_ in df_list[1:]:
                    df = df.unionByName(
                        df_
                    )  # Usa unionByName para combinar os DataFrames

                return df

    else:
        # Processando de maneira manual, com datas fixas sendo passadas no pipeline_options no arquivo da DAG
        start_date = dbutils.widgets.get("start_date")  # type: ignore
        end_date = dbutils.widgets.get("end_date")  # type: ignore
        logging.info(
            f"Processando de {start_date} até {end_date} de forma manual..."
        )

        query = f"""
            SELECT
                *
            FROM
                fintalk-datalake-prd.alloha.vw_{table_name}
            WHERE
                created_at >= {start_date} AND created_at < {end_date}
        """

        return collect_and_transform_data(credentials, query, table_name)


def save_to_landing(
    df: DataFrame,
    widgets: dict,
    schema_name: str,
    table_name: str,
):
    # Checa se há dados existentes para salvar no Data Lake
    if df.count() == 0:
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

    # Salva os dados como parquet no Data Lake
    DataLake.write_table_as_parquet(
        df=df_with_metadatas.repartition(1), dl_path=dl_path
    )
    logging.info(
        f"{table_name} foi appendado no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Fintalk a partir do AWS Secrets Manager
    credentials = SecretsManager().get_secrets(secret_name="fintalk")

    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            df = collect_data(
                credentials=credentials,
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
                about_ingestion=about_ingestion,
            )
            save_to_landing(
                df=df,
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
            )  # Salvando os dados transformados em DataFrame (Spark) na camada landing do S3


if __name__ == "__main__":
    main()
