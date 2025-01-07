# Databricks notebook source

%pip install firebase-admin google-cloud-firestore
%pip install protobuf==3.20.1

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from pyspark.sql.types import StructType, StructField, StringType
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging
from datetime import datetime, timedelta
from google.api_core import retry


# Configuração dos logs
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")



def fetch_firestore_sample_data_sessions(table_name, about_ingestion):
    # Chama a função de inicialização do Firebase

    db = firestore.client()
    collection_ref = db.collection(table_name)
    print(table_name)
    incremental_date = datetime.now() - timedelta(days=1)
    start_date = (incremental_date.strftime('%d-%m-%Y 00:00:00'))
    end_date = (incremental_date.strftime('%d-%m-%Y 23:59:59'))
    query = (
        collection_ref.where(filter=FieldFilter(about_ingestion["incremental_col"], ">=", start_date))
            .where(filter=FieldFilter(about_ingestion["incremental_col"], "<=", end_date)
            )
    )
    docs = query.stream(retry=retry.Retry(deadline=60))
    data = []
    for doc in docs:
        doc_dict = doc.to_dict()
        doc_dict['sessions_id'] = doc.id
        data.append(doc_dict)
    for doc in data:
        print(doc)
    if data:
        column_names = list(data[0].keys())
    else:
        raise ValueError("Nenhum documento encontrado para a consulta especificada.")

    # Define o schema para o DataFrame do Spark
    schema = StructType([StructField(name, StringType(), True) for name in column_names])

    # Cria o DataFrame Spark com os dados
    df = spark.createDataFrame(data, schema=schema)
    df = df.dropDuplicates()

    return df


def fetch_firestore_sample_data_events(table_name, about_ingestion):
    # Chama a função de inicialização do Firebase

    db = firestore.client()
    events_ref = db.collection(table_name)
    print(table_name)
    incremental_date = datetime.now() - timedelta(days=1)
    start_date = (incremental_date.strftime('%d-%m-%Y 00:00:00'))
    end_date = (incremental_date.strftime('%d-%m-%Y 23:59:59'))
    query_events = (
            events_ref.order_by("createdAt")
            .where(filter=FieldFilter(about_ingestion["incremental_col"], ">=", start_date))
            .where(filter=FieldFilter(about_ingestion["incremental_col"], "<=", end_date))
        )
    docs = query_events.stream(retry=retry.Retry(deadline=60))
    data = []
    for doc in docs:
        doc_dict = doc.to_dict()
        data.append(doc_dict)
    for doc in data:
        print(doc)

    # Extrai os nomes das colunas do primeiro documento
    if data:
        column_names = list(data[0].keys())
    else:
        raise ValueError("Nenhum documento encontrado para a consulta especificada.")

    # Define o schema para o DataFrame do Spark
    schema = StructType([StructField(name, StringType(), True) for name in column_names])

    # Remover a coluna 'dateOfCreation' de cada dicionário
    data = [{key: value for key, value in doc.items() if key != 'dateOfCreation'} for doc in data]

    # Atualizar o esquema após remover a coluna
    column_names = list(data[0].keys()) if data else []
    schema = StructType([StructField(name, StringType(), True) for name in column_names])

    # Cria o DataFrame Spark com os dados
    df = spark.createDataFrame(data, schema=schema)
    df = df.dropDuplicates()
    df.show()
    return df


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
        df=df_with_metadatas, dl_path=dl_path, mode = "append"
    )
    logging.info(
        f"A tabela {table_name} foi appendado no Data Lake com sucesso em {dl_path}"
    )


def main():
    # Obtendo widgets do Databricks
    widgets = DataLake.get_landing_widgets_from_databricks_v2()

    # Obtendo credenciais do Five9 a partir do Databricks Secrets
    auth = eval(dbutils.widgets.get("APP_AUTH_FIREBASE"))  # type: ignore
    if not firebase_admin._apps:
        cred = credentials.Certificate(auth)
        firebase_admin.initialize_app(cred)


    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            if table_name == "sessions":
                df = fetch_firestore_sample_data_sessions(table_name, about_ingestion)
                save_to_s3_spark(df,widgets, schema_name, table_name)
            else:
                df = fetch_firestore_sample_data_events(table_name, about_ingestion)
                save_to_s3_spark(df,widgets, schema_name, table_name)


if __name__ == "__main__":
    main()
