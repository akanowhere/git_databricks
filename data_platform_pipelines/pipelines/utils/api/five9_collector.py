import base64
import csv
import logging
import time
from datetime import datetime, timezone
from io import StringIO

import xmltodict
from databricks.sdk.runtime import *  # type: ignore
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType
from zoneinfo import ZoneInfo

from data_platform_pipelines.pipelines.utils.api.handler_http import (
    HTTPMETHODS,
    HTTPRequest,
    make_http_request,
)


class Five9Collector:
    """
    Classe para coleta de dados de relatórios do Five9.

    Args:
        folder_name (str): Nome da pasta onde o relatório está localizado no Five9.
        report_name (str): Nome do relatório a ser obtido.
        startAt (str): Data de início do período do relatório (no formato "YYYY-MM-DD").
        endAt (str): Data de término do período do relatório (no formato "YYYY-MM-DD").
    """

    def __init__(
        self,
        folder_name: str,
        report_name: str,
        startAt: str,
        endAt: str,
        url: str,
        client_id: str,
        client_secret: str,
    ):
        """Inicializa a classe APICollector com os parâmetros fornecidos."""
        self.folder_name = folder_name
        self.report_name = report_name
        self.startAt = startAt
        self.endAt = endAt
        self.url = url
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = self.get_headers()

    def get_headers(self) -> dict:
        """
        Obtém os cabeçalhos HTTP necessários para a solicitação à API.

        Returns:
            Dict[str, str]: Os cabeçalhos HTTP.
        """
        auth_header = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("utf-8")

        headers = {
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/xml;charset=utf-8",
            "Accept-Encoding": "gzip, deflate, br",
            "SOAPAction": "",
        }

        return headers

    def get_report_id(self) -> str:
        """
        Obtém o identificador únicos dos relatórios da Five9.

        Returns:
            str: Identificador único do relatório.
        """
        data = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:runReport>
                        <folderName>{self.folder_name}</folderName>
                        <reportName>{self.report_name}</reportName>
                        <criteria>
                            <time>
                                <end>{self.endAt}</end>
                                <start>{self.startAt}</start>
                            </time>
                        </criteria>
                    </ser:runReport>
                </soapenv:Body>
            </soapenv:Envelope>
        """
        response = make_http_request(
            HTTPRequest(
                url=self.url,
                method=HTTPMETHODS.POST,
                headers=self.headers,
                data=data,
            )
        )

        return response

    def is_report_running(self, report_id: str) -> str:
        """
        Verifica se um relatório está em execução.

        Args:
            report_id (str): Identificador único do relatório.

        Returns:
            str: A resposta da solicitação HTTP.
        """
        data = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:isReportRunning>
                        <identifier>{report_id}</identifier>
                    </ser:isReportRunning>
                </soapenv:Body>
            </soapenv:Envelope>
        """
        response = make_http_request(
            HTTPRequest(
                url=self.url,
                method=HTTPMETHODS.POST,
                headers=self.headers,
                data=data,
            )
        )

        return response

    def get_data(self, report_id: str) -> str:
        """
        Obtém os dados de um relatório.

        Args:
            report_id (str): Identificador único do relatório.

        Returns:
            str: Os dados do relatório no formato CSV.
        """
        data = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:getReportResultCsv>
                        <identifier>{report_id}</identifier>
                    </ser:getReportResultCsv>
                </soapenv:Body>
            </soapenv:Envelope>
        """
        response = make_http_request(
            HTTPRequest(
                url=self.url,
                method=HTTPMETHODS.POST,
                headers=self.headers,
                data=data,
            )
        )

        return response

    def xmltodict_parse(self, response: str, response_type: str) -> str:
        """
        Faz o parse de uma resposta XML para um dicionário usando xmltodict, com base no tipo especificado.

        Args:
            response (str): A resposta XML a ser analisada.
            type (str): O tipo de método da requisição ("get_report_id", "is_report_running" ou "get_data").

        Returns:
            Union[str, bool]: O resultado do parse para dicionário do XML ou False em caso de erro.
        """
        try:
            response_mapping = {
                "get_report_id": "env:Envelope.env:Body.ns2:runReportResponse.return",
                "is_report_running": "env:Envelope.env:Body.ns2:isReportRunningResponse.return",
                "get_data": "env:Envelope.env:Body.ns2:getReportResultCsvResponse.return",
            }
            keys = response_mapping.get(response_type)
            if keys is None:
                logging.error(f"Argumento inválido: {response_type}")
                raise

            value = xmltodict.parse(response.text)
            for key in keys.split("."):
                value = value[key]

            return value

        except KeyError as e:
            logging.error(f"Chave não encontrada na resposta XML: {e}")
            raise
        except Exception as e:
            logging.error(f"Erro ao fazer o parse da resposta XML: {e}")
            raise

    def transform_csv_to_df(self, string_csv: str) -> DataFrame:
        """
        Transforma uma string CSV em um DataFrame Spark.

        Args:
            string_csv (str): Uma string contendo os dados CSV.

        Returns:
            DataFrame: Um DataFrame Spark contendo os dados CSV.
        """
        try:
            # Usa o StringIO para criar um objeto semelhante a um arquivo a partir da string
            csv_file = StringIO(string_csv)
            data = csv.reader(csv_file)  # Lê o conteúdo CSV

            # Extrai o cabeçalho e as linhas do CSV
            header = [
                column_name.replace(" ", "_") for column_name in next(data)
            ]
            rows = [row for row in data if len(row) == len(header)]

            # Define o esquema do DataFrame com base no cabeçalho
            schema = StructType([
                StructField(col, StringType(), True) for col in header
            ])

            # Cria o DataFrame Spark
            df = spark.createDataFrame(rows, schema)  # type: ignore

            return df
        except Exception as e:
            logging.error(
                f"Erro ao tentar transformar a string CSV em DataFrame Spark. Motivo do erro: {e}"
            )
            raise

    def start(self) -> DataFrame:
        """
        Inicia o processo de extração de dados do relatório do Five9.

        Returns:
            Union[pd.DataFrame, None]: Um DataFrame contendo os dados do relatório ou None em caso de falha.
        """
        try:
            report_id = self.xmltodict_parse(
                self.get_report_id(), "get_report_id"
            )
            while True:
                time.sleep(15)
                if (
                    self.xmltodict_parse(
                        self.is_report_running(report_id),
                        "is_report_running",
                    )
                    == "true"
                ):
                    time.sleep(1)
                else:
                    string_csv = self.xmltodict_parse(
                        self.get_data(report_id), "get_data"
                    )
                    break

            # Transforma a string CSV em DataFrame Spark
            df = self.transform_csv_to_df(string_csv)

            return df
        except Exception as e:
            logging.error(
                f"Erro ao iniciar o processo de obtenção de dados do relatório: {e}"
            )
            raise


def get_timezone_diff_and_timenow(
    source_tz_str: str = "US/Pacific",
    target_tz_str: str = "America/Sao_Paulo",
):
    # Obter a hora atual no horário UTC
    utc_now = datetime.now(timezone.utc)

    # Converter a hora atual para os timezones desejados
    source_time = utc_now.astimezone(ZoneInfo(source_tz_str))
    target_time = utc_now.astimezone(ZoneInfo(target_tz_str))

    # Calcular a diferença entre os dois horários em horas
    timezone_diff = int(
        (target_time.utcoffset() - source_time.utcoffset()).total_seconds()
        / 3600
    )

    return timezone_diff, target_time
