# Databricks notebook source



%pip install google-cloud-storage


import json
from io import StringIO
from pyspark.sql.types import StructType, StructField, StringType
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging
from datetime import datetime, timedelta
from google.api_core import retry
from google.cloud import storage
from google.oauth2 import service_account
import os
import pandas as  pd
from google.cloud import storage
from google.oauth2 import service_account
import os
import pandas as p
from pyspark.sql import SparkSession



# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")



def fetch_stats_charge(table_name, about_ingestion):
        # Configuração do Spark
    spark = SparkSession.builder.getOrCreate()


      # Carregar as credenciais JSON do Airflow
    auth_json = dbutils.widgets.get("APP_AUTH_LOJA")  # Obtém o JSON como uma string

    # Se auth_json é uma string JSON, converta-a em dicionário
    auth = eval(auth_json)  # Assume que o JSON é uma string que pode ser avaliada como dicionário

    # Verifique se auth é um dicionário

    credentials = service_account.Credentials.from_service_account_info(auth)
    client = storage.Client(credentials=credentials, project=auth["project_id"])


    # Lista dos nomes dos buckets
    bucket_names = [
        'pubsite_prod_rev_04297449565324755563'  # Mob
    ]

    # Caminho do subdiretório dentro do bucket
    subdirectory = 'stats/installs'

    # Prefixo e sufixo para os arquivos
    prefixo = 'installs_br.'
    sufixo = '_overview.csv'



    pandas_df_total = pd.DataFrame()

    # Itera sobre os buckets
    for bucket_name in bucket_names:

        # Acessa o bucket
        bucket = client.get_bucket(bucket_name)

        # Lista os blobs (arquivos) no subdiretório 'stats/installs'
        blobs = bucket.list_blobs(prefix=subdirectory + '/')

        # Faz o download dos arquivos CSV e os transforma em DataFrames Spark
        for blob in blobs:
            if blob.name.endswith(sufixo):
                # Lê o conteúdo do arquivo em um DataFrame Pandas
                data = blob.download_as_string().decode('utf-16-le')
                data_io = StringIO(data)
                pandas_df = pd.read_csv(data_io)

                # Converte o DataFrame Pandas para um DataFrame Spark
                pandas_df.rename(columns={'Package Name': 'Package name'}, inplace=True)
                pandas_df.rename(columns={'Package name': 'Package name'}, inplace=True)
                pandas_df_total = pd.concat([pandas_df_total, pandas_df], ignore_index=True)
                for col in pandas_df.columns:
                    if col in pandas_df_total.columns:
                        pandas_df = pandas_df.drop(columns=[col])

    spark_df = spark.createDataFrame(pandas_df_total)
    return spark_df



def fetch_ratings_charge(table_name, about_ingestion):
        # Configuração do Spark
    spark = SparkSession.builder.getOrCreate()


      # Carregar as credenciais JSON do Airflow
    auth_json = dbutils.widgets.get("APP_AUTH_LOJA")  # Obtém o JSON como uma string

    # Se auth_json é uma string JSON, converta-a em dicionário
    auth = eval(auth_json)  # Assume que o JSON é uma string que pode ser avaliada como dicionário

    # Verifique se auth é um dicionário

    credentials = service_account.Credentials.from_service_account_info(auth)
    client = storage.Client(credentials=credentials, project=auth["project_id"])


    # Lista dos nomes dos buckets
    bucket_names = [
        'pubsite_prod_rev_04297449565324755563'  # Mob
    ]

    # Caminho do subdiretório dentro do bucket
    subdirectory = 'stats/ratings'

    # Prefixo e sufixo para os arquivos
    prefixo = 'installs_br.'
    sufixo = '_overview.csv'

    pandas_df_total = pd.DataFrame()

    # Itera sobre os buckets
    for bucket_name in bucket_names:

        # Acessa o bucket
        bucket = client.get_bucket(bucket_name)

        # Lista os blobs (arquivos) no subdiretório 'stats/installs'
        blobs = bucket.list_blobs(prefix=subdirectory + '/')

        # Faz o download dos arquivos CSV e os transforma em DataFrames Spark
        for blob in blobs:
            if blob.name.endswith(sufixo):
                # Lê o conteúdo do arquivo em um DataFrame Pandas
                data = blob.download_as_string().decode('utf-16-le')
                data_io = StringIO(data)
                pandas_df = pd.read_csv(data_io)

                # Converte o DataFrame Pandas para um DataFrame Spark
                pandas_df.rename(columns={'Package Name': 'Package name'}, inplace=True)
                pandas_df.rename(columns={'Package name': 'Package name'}, inplace=True)
                pandas_df_total = pd.concat([pandas_df_total, pandas_df], ignore_index=True)
                for col in pandas_df.columns:
                    if col in pandas_df_total.columns:
                        pandas_df = pandas_df.drop(columns=[col])

    spark_df = spark.createDataFrame(pandas_df_total)

    return spark_df



def save_to_s3_spark(df,widgets, schema_name, table_name):
     # Adiciona campos técnicos ao dataframe
    metadatas = {
        "_data_source": widgets["metadata"]["data_source"],
        "_source_type": widgets["metadata"]["source_type"],
        "_cost_center": widgets["metadata"]["cost_center"],
        "_system_owner": widgets["metadata"]["system_owner"],
    }
    df_with_metadatas = DataLake.add_metadata_v2(
        df, metadatas
    )

    # Cria o caminho para salvar os dados no Data Lake
    dl_path = widgets["outputs"].format(
        source_type=widgets["source_type"],
        data_source=widgets["data_source"],
        schema=schema_name,
        table=table_name,
    )

    # Salva os dados como csv no Data Lake
    DataLake.write_table_as_csv(
        df=df_with_metadatas, dl_path=dl_path, mode = "overwrite"
    )
    logging.info(
        f"A tabela {table_name} foi appendado no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Five9 a partir do Databricks Secrets

    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            print(table_name)
            if table_name == "loja_stats":
                df = fetch_stats_charge(table_name, about_ingestion)
                save_to_s3_spark(df, widgets, schema_name, table_name)
            else:
                df = fetch_ratings_charge(table_name, about_ingestion)
                save_to_s3_spark(df, widgets, schema_name, table_name)



if __name__ == "__main__":
    main()


# subdirectory = 'stats/ratings'

