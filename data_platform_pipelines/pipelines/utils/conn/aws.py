import boto3
import json
import base64
from botocore.config import Config
from databricks.sdk.runtime import *


class AwsSession:
    """
    Classe para criar e gerenciar sessões AWS usando credenciais obtidas de um
    escopo de segredos.

    Atributos:
        max_pool_connections (int): Número máximo de conexões no pool.
        region_name (str): Nome da região AWS a ser usada.
        aws_access_key_id (str): Chave de acesso AWS.
        aws_secret_access_key (str): Chave secreta AWS.
        config (Config): Configuração personalizada para o cliente AWS.
    """

    def __init__(self, region_name: str = "us-east-1") -> None:
        """
        Inicializa uma instância da classe AwsSession.

        Args:
            region_name (str, opcional): Nome da região AWS a ser usada. Padrão
            é "us-east-1".
        """
        self.region_name = region_name
        self.aws_access_key_id = dbutils.secrets.get(
            scope="service-account", key="aws_access_key_id"
        )
        self.aws_secret_access_key = dbutils.secrets.get(
            scope="service-account", key="aws_secret_access_key"
        )

    def _create_session(self) -> boto3.Session:
        """
        Cria uma nova sessão boto3 com as credenciais e a região fornecidas.

        Returns:
            boto3.Session: Uma nova sessão boto3.
        """
        return boto3.Session(
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def get_client_to(
        self, service: str, max_pool_connections: int = None
    ) -> object:
        """
        Obtém um cliente para o serviço AWS especificado.

        Args:
            service (str): O nome do serviço AWS para o qual se deseja obter um
            cliente (e.g., 's3', 'ec2').

            max_pool_connections (int, opcional): Número máximo de conexões no
            pool. Padrão é None.

        Returns:
            object: Cliente boto3 para o serviço especificado.
        """
        session = self._create_session()

        if max_pool_connections:
            config = Config(max_pool_connections=max_pool_connections)

            return session.client(service, config=config)

        else:
            return session.client(service)


class SecretsManager:
    def __init__(self, region_name: str = "us-east-1") -> None:
        """
        Inicializa uma instância da classe AwsSession.

        Args:
            region_name (str, opcional): Nome da região AWS a ser usada. Padrão
            é "us-east-1".
        """
        self.region_name = region_name
        self.aws_access_key_id = dbutils.secrets.get(
            scope="secrets_manager", key="aws_access_key_id"
        )
        self.aws_secret_access_key = dbutils.secrets.get(
            scope="secrets_manager", key="aws_secret_access_key"
        )
        self.secret_client = self._create_client()

    def _create_client(self) -> boto3.client:
        """
        Cria um novo cliente boto3 com as credenciais e a região fornecidas.

        Returns:
            boto3.client: Um novo cliente boto3.
        """
        return boto3.client(
            "secretsmanager",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def get_secrets(self, secret_name: str) -> dict:  # noqa
        """Returns the value of a secret given its name if it exists in the secrets manager.

        Args:
            secret_name (str): Name of the secret to be returned.

        Raises:
            DecryptionFailureException: Secrets Manager cannot decrypt secret value using KMS key.
            InternalServiceErrorException: There was an error on the server.
            InvalidParameterException: Invalid informed parameter.
            InvalidRequestException: Invalid parameter for the current state of the resource.
            ResourceNotFoundException: Resource not found.

        Returns:
            secret(dict): Secret key value dictionary.
        """
        try:
            get_secret_value_response = self.secret_client.get_secret_value(
                SecretId=secret_name
            )
        except Exception as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                raise e
            elif (
                e.response["Error"]["Code"] == "InternalServiceErrorException"
            ):
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise e
        else:
            if "SecretString" in get_secret_value_response:
                return json.loads(get_secret_value_response["SecretString"])
            else:
                return json.loads(
                    base64.b64decode(get_secret_value_response["SecretBinary"])
                )
        return {}
