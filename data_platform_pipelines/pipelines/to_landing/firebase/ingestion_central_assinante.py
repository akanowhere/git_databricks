# Databricks notebook source

%pip install firebase-admin google-cloud-firestore
%pip install protobuf==3.20.1
%pip install firebase-admin google-cloud-firestore
%pip install protobuf==3.20.1
%pip install google-cloud-bigquery
%pip install google-auth==2.34.0
%pip install pandas 
%pip install pytz


from pyspark.sql import SparkSession
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from pyspark.sql.types import StructType, StructField, StringType
from data_platform_pipelines.pipelines.utils.datalake import DataLake
import logging
from datetime import datetime, timedelta
from google.api_core import retry
import os
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import pytz
import pandas as pd




# Configuração dos logs                     
logger = spark._jvm.org.apache.log4j  # type: ignore
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")




def fetch_firestore_sample_data_sessions(table_name, about_ingestion):

    project_id = 'alloha-app-producao'
    database_id = 'alloha-central-do-assinante-web'
    collection = table_name

    now = datetime.now() - timedelta(days=1)
    formatted_date_year = int(now.strftime('%Y'))
    formatted_date_month = int(now.strftime('%m'))
    formatted_date_day = int(now.strftime('%d'))



    timezone = pytz.timezone('America/Sao_Paulo')
    start_date = timezone.localize(datetime(formatted_date_year, formatted_date_month, formatted_date_day, 0, 0, 0))
    end_date = timezone.localize(datetime(formatted_date_year, formatted_date_month, formatted_date_day, 23, 59, 59))

    print(start_date)
    print(end_date)
    auth = eval(dbutils.widgets.get("APP_AUTH_CENTRAL_ASSINANTE"))  # type: ignore
    # if not firebase_admin._apps:
    #     cred = credentials.Certificate(auth)
    #     firebase_admin.initialize_app(cred) 
    
    credentials = service_account.Credentials.from_service_account_info(
        auth,
        scopes=["https://www.googleapis.com/auth/datastore"]
    )


    # Chama a função de inicialização do Firebase
    credentials.refresh(Request())  # Atualizar o token de autenticação
    token = credentials.token

    # Formatação de data para strings no Firestore
    start_date_str = start_date.astimezone(pytz.timezone("America/Sao_Paulo")).isoformat()
    end_date_str = end_date.astimezone(pytz.timezone("America/Sao_Paulo")).isoformat()

    # Endpoint correto para consultar documentos no Firestore com filtros
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents:runQuery"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    query = {
        "structuredQuery": {
            "from": [{"collectionId": collection}],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": about_ingestion["incremental_col"]},
                                "op": "GREATER_THAN_OR_EQUAL",
                                "value": {"stringValue": start_date_str}
                            }
                        },
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": about_ingestion["incremental_col"]},
                                "op": "LESS_THAN_OR_EQUAL",
                                "value": {"stringValue": end_date_str}
                            }
                        }
                    ]
                }
            }
        }
    }

    # Fazer a requisição para obter apenas os documentos filtrados
    response = requests.post(url, headers=headers, json=query)

    # Exibir o status e o conteúdo da resposta para diagnóstico
    #print(f"Status da resposta: {response.status_code}")
    

    if response.status_code == 200:
        rows = []
        for result in response.json():
            doc = result.get("document")
            if doc:
                fields = doc.get('fields', {})
                row = {key: value.get('stringValue') or value.get('integerValue') or value.get('doubleValue')
                       for key, value in fields.items()}
                row['sessions_id'] = doc.get('name').split('/')[-1]  # Extrair o ID do documento
                rows.append(row)
        
        # Converter os dados filtrados diretamente para um DataFrame
        df_pandas = pd.DataFrame(rows)
        df_pandas['createdAt'] = df_pandas['createdAt'].str.slice(0, 19)
        print(df_pandas)
        spark = SparkSession.builder.appName("FirestoreDataProcessing").getOrCreate()

        # Converter o DataFrame Pandas para DataFrame Spark
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.show(truncate=False)
        return df_spark
    else:
        print(f"Erro: {response.status_code} - {response.text}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro


def fetch_firestore_sample_data_events(table_name, about_ingestion):
    # Chama a função de inicialização do Firebase
    project_id = 'alloha-app-producao'
    database_id = 'alloha-central-do-assinante-web'
    collection = table_name

    now = datetime.now() - timedelta(days=1)
    timezone = pytz.timezone('America/Sao_Paulo')
    start_date = timezone.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    end_date = timezone.localize(datetime(now.year, now.month, now.day, 23, 59, 59))

    start_date_str = start_date.astimezone(pytz.utc).isoformat()
    end_date_str = end_date.astimezone(pytz.utc).isoformat()

    auth = eval(dbutils.widgets.get("APP_AUTH_CENTRAL_ASSINANTE"))
    credentials = service_account.Credentials.from_service_account_info(
        auth, scopes=["https://www.googleapis.com/auth/datastore"]
    )
    credentials.refresh(Request())
    token = credentials.token

    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents:runQuery"
    headers = {'Authorization': f'Bearer {token}'}
    query = {
        "structuredQuery": {
            "from": [{"collectionId": collection}],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": about_ingestion["incremental_col"]},
                                "op": "GREATER_THAN_OR_EQUAL",
                                "value": {"stringValue": start_date_str}
                            }
                        },
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": about_ingestion["incremental_col"]},
                                "op": "LESS_THAN_OR_EQUAL",
                                "value": {"stringValue": end_date_str}
                            }
                        }
                    ]
                }
            }
        }
    }

    # Fazer a requisição para obter apenas os documentos filtrados
    response = requests.post(url, headers=headers, json=query)

    # Exibir o status e o conteúdo da resposta para diagnóstico
    #print(f"Status da resposta: {response.status_code}")
    

    if response.status_code == 200:
        rows = []
        for result in response.json():
            doc = result.get("document")
            if doc:
                fields = doc.get('fields', {})
                row = {key: value.get('stringValue') or value.get('integerValue') or value.get('doubleValue')
                       for key, value in fields.items()}
                rows.append(row)

        df_pandas = pd.DataFrame(rows)
        df_pandas['createdAt'] = df_pandas['createdAt'].str.slice(0, 19)
        print(df_pandas)

        spark = SparkSession.builder.appName("FirestoreDataProcessing").getOrCreate()
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.show(truncate=False)
        return df_spark
    else:
        print(f"Erro: {response.status_code} - {response.text}")
        return pd.DataFrame()
    


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
    #auth = eval(dbutils.widgets.get("APP_AUTH_CENTRAL_ASSINANTE"))  # type: ignore



    for schema_name, tables_info in widgets["schemas"].items():
        for table_name, about_ingestion in tables_info.items():
            if table_name == "sessions":
                df = fetch_firestore_sample_data_sessions(table_name, about_ingestion)
                save_to_s3_spark(df,widgets, schema_name, table_name)
            else:
                df = fetch_firestore_sample_data_events(table_name, about_ingestion)
                save_to_s3_spark(df,widgets, schema_name, table_name)
                print("FINALIZADO")


if __name__ == "__main__":
    main()
