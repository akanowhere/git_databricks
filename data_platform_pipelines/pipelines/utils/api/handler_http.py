"""Módulo de manipulação de solicitações HTTP."""

import logging
from enum import Enum
from typing import Dict, Optional

import httpx
from pydantic import BaseModel, validator


class HTTPMETHODS(Enum):
    """Esta classe é uma enumeração de métodos HTTP."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class HTTPRequest(BaseModel):
    """
    Classe que representa uma solicitação HTTP genérica.

    Args:
        url (str): A URL da solicitação.
        method (str): O método HTTP a ser usado (padrão é "GET").
        headers (dict): Um dicionário de cabeçalhos HTTP personalizado.
        params (dict): Um dicionário de parâmetros de consulta.
        data (str): Uma string de dados a serem enviados na solicitação.
    """

    url: str
    method: HTTPMETHODS = HTTPMETHODS.GET
    headers: Optional[Dict] = None
    params: Optional[Dict] = None
    data: Optional[str] = None

    @validator("headers", "params")
    def set_default_dict(cls, value, field):
        if value is None:
            value = {}
        return value


def make_http_request(http_request: HTTPRequest) -> httpx.Response:
    """
    Faz uma solicitação HTTP genérica.

    Args:
        http_request (HTTPRequest): Um objeto HTTPRequest contendo os detalhes da solicitação.

    Returns:
        httpx.Response: A resposta da solicitação HTTP.
    """
    try:
        with httpx.Client() as client:
            response = client.request(
                http_request.method.value,
                http_request.url,
                headers=http_request.headers,
                params=http_request.params,
                data=http_request.data,
            )
            response.raise_for_status()
            return response
    except httpx.HTTPStatusError as e:
        logging.error(
            f"Erro na solicitação HTTP: {e.response.status_code} - {e.response.text}"
        )
        raise
    except httpx.RequestError as e:
        logging.error(f"Erro na requisição HTTP: {e}")
        raise
