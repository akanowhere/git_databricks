import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials


class BigQueryCollector:
    """Classe responsável pela coleta de dados do BigQuery."""

    def __init__(self, service_account_info: dict, query: str):
        """Inicializa uma instância da classe BigQueryCollector."""
        self.service_account_info = service_account_info
        self.query = query

    @staticmethod
    def _get_credentials(service_account_info: dict) -> Credentials:
        """Obtém as credenciais do usuário através da service-account-key.

        Returns:
            Credentials: As credenciais do usuário.
        """
        return service_account.Credentials.from_service_account_info(
            service_account_info
        )

    @staticmethod
    def _create_client(creds: Credentials) -> bigquery.Client:
        """Cria um novo cliente BigQuery com as credenciais fornecidas.

        Returns:
            bigquery.Client: Um novo cliente BigQuery.
        """
        return bigquery.Client(credentials=creds, project=creds.project_id)

    @staticmethod
    def _collect_data(client: bigquery.Client, query: str) -> list:
        """Executa a consulta SQL no BigQuery e extrai os dados.

        Returns:
            list: Uma lista de dicionários com os dados extraídos.
        """
        try:
            query_job = client.query(query)
            results = query_job.result()
            data = [dict(row) for row in results]
            logging.info("Dados coletados do BigQuery com sucesso.")

            return data
        except Exception as e:
            logging.error(f"Erro ao coletar dados do BigQuery: {e}")
            raise

    def start(self) -> list:
        """Inicia a coleta de dados do BigQuery."""
        creds = self._get_credentials(self.service_account_info)
        client = self._create_client(creds)
        return self._collect_data(client, self.query)
