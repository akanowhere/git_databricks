# Databricks notebook source

%pip install xmltodict==0.13.0  # type: ignore
%pip install httpx==0.27.0  # type: ignore


import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame

from data_platform_pipelines.pipelines.utils.api.five9_collector import (
    Five9Collector,
    get_timezone_diff_and_timenow,
)
from data_platform_pipelines.pipelines.utils.conn.aws import SecretsManager
from data_platform_pipelines.pipelines.utils.datalake import DataLake

# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# Função auxiliar para gerar intervalos de tempo
def generate_time_intervals(
    start_date: datetime, end_date: datetime, time_intervals: int
) -> list:
    """Gera uma lista de intervalos de tempo de acordo com o intervalo de horas especificado."""
    intervals = []
    current_start = start_date

    # Definir a duração das horas com base no número de intervalos
    if time_intervals == 1:
        hours_per_interval = 23
    elif time_intervals == 2:
        hours_per_interval = 11
    elif time_intervals == 3:
        hours_per_interval = 7
    else:
        raise ValueError("time_intervals deve ser 1, 2 ou 3")

    while current_start < end_date:
        for i in range(time_intervals):
            # Definir intervalos de horas de acordo com time_intervals
            current_end = current_start + timedelta(
                hours=hours_per_interval,
                minutes=59,
                seconds=59,
                microseconds=999,
            )
            current_end = min(current_end, end_date)  # Ajusta o intervalo final para não ultrapassar o end_date
            intervals.append((
                current_start.strftime("%Y-%m-%dT%H:%M:%S.000-03:00"),
                current_end.strftime("%Y-%m-%dT%H:%M:%S.999-03:00"),
            ))
            current_start = current_end + timedelta(seconds=1)

            if current_start >= end_date:  # Saída se ultrapassar o end_date
                break

    return intervals


def collect_data(
    url: str,
    client_id: str,
    client_secret: str,
    widgets: dict,
    schema_name: str,
    table_name: str,
    about_ingestion: dict,
    timenow: datetime,
) -> DataFrame:
    logging.info(
        f"Processando {widgets['source_type']}.{widgets['data_source']}.{schema_name}.{table_name}..."
    )

    # Determinar start_date e end_date
    if widgets["incremental"]:
        # Obtendo o timestamp da última atualiazação incremental
        last_update = DataLake.get_max_timestamp_from_delta_table(
            catalog=widgets["catalog"],
            schema=schema_name,
            table=table_name,
            incremental_col=about_ingestion["incremental_col"],
        )
        logging.info(f"Última atualização incremental: {last_update}")

        if (
            widgets["frequency"] == "hourly"
        ):  # Processando incrementalmente de forma horária
            start_date = last_update.strftime("%Y-%m-%dT%H:%M:%S.%f-03:00")
            end_date = timenow.strftime("%Y-%m-%dT%H:%M:%S.%f-03:00")
            logging.info(
                f"Processando incrementalmente de {start_date} até {end_date} de forma horária..."
            )
        else:  # Processando incrementalmente de forma diária
            # Obtendo "start_date" e "end_date" de acordo com o parâmetro "days" de "daily_extract"
            days = about_ingestion["daily_extract"]["days"]
            start_date = (
                (last_update - timedelta(days=(days - 1)))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .replace(tzinfo=timezone(timedelta(hours=-3)))
            )

            # Obtém intervalos de 12h de diferença para start_date e end_date
            intervals = generate_time_intervals(
                start_date=start_date,
                end_date=timenow,
                time_intervals=about_ingestion["time_intervals"],
            )
    else:
        # Processando de maneira manual, com datas fixas sendo passadas no pipeline_options no arquivo da DAG
        start_date = dbutils.widgets.get("start_date")  # type: ignore
        end_date = dbutils.widgets.get("end_date")  # type: ignore
        logging.info(
            f"Processando de {start_date} até {end_date} de forma manual..."
        )

    # Coleta de dados para incremento diário
    if widgets["incremental"] and widgets["frequency"] == "daily":
        # Inicializa a coleta de dados para cada intervalo de start_date e end_date
        df_list = []
        for start_date, end_date in intervals:
            logging.info(
                f"Processando incrementalmente de {start_date} até {end_date}..."
            )
            df = Five9Collector(
                folder_name=about_ingestion["folder_name"],
                report_name=table_name,
                startAt=start_date,
                endAt=end_date,
                url=url,
                client_id=client_id,
                client_secret=client_secret,
            ).start()

            # Adiciona o DataFrame coletado à lista de dataframes
            df_list.append(df)

        # Realizando o union dos DataFrames coletados
        if df_list:
            df = df_list[0]
            for df_ in df_list[1:]:
                df = df.unionByName(
                    df_
                )  # Usa unionByName para combinar os DataFrames

            return df

    # Coleta de dados para incremento horário ou manual
    df = Five9Collector(
        folder_name=about_ingestion["folder_name"],
        report_name=table_name,
        startAt=start_date,
        endAt=end_date,
        url=url,
        client_id=client_id,
        client_secret=client_secret,
    ).start()

    return df


def save_to_landing(
    df: DataFrame,
    widgets: dict,
    schema_name: str,
    table_name: str,
    timezone_diff: int,
    timenow: datetime,
):
    # Checa se há dados existentes para salvar no Data Lake
    if df.count() == 0:
        logging.warning(
            f"{table_name} não possui dados disponíveis para o período especificado.."
        )
        return

    # Adiciona campos técnicos ao dataframe
    metadatas = {
        "_timezone_diff": timezone_diff,
        "_extraction_date": timenow.strftime("%Y-%m-%d %H:%M:%S"),
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
    DataLake.write_table_as_parquet(
        df=df_with_metadatas.repartition(1), dl_path=dl_path
    )
    logging.info(
        f"{table_name} foi appendado no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Five9 a partir do AWS Secrets Manager
    credentials = SecretsManager().get_secrets("five9")

    # Obter os campos timezone_diff e timenow (extract_date)
    timezone_diff, timenow = get_timezone_diff_and_timenow()

    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            df = collect_data(
                **credentials,
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
                about_ingestion=about_ingestion,
                timenow=timenow,
            )
            save_to_landing(
                df=df,
                widgets=widgets,
                schema_name=schema_name,
                table_name=table_name,
                timezone_diff=timezone_diff,
                timenow=timenow,
            )


if __name__ == "__main__":
    main()
