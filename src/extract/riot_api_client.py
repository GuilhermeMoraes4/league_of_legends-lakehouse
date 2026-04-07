"""
Cliente HTTP para a Riot Games API.

Responsabilidades:
- Rate limiting (respeitando 20 req/s e 100 req/2min da dev key)
- Exponential backoff em caso de 429 (Too Many Requests)
- Retry automatico em erros transientes (5xx)
- Logging estruturado para debug
"""

import logging
import time

import requests as http_requests

from src.extract.config import (
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    SAFE_DELAY_BETWEEN_REQUESTS,
    get_api_key,
)

logger = logging.getLogger(__name__)


class RiotAPIClient:
    """
    Client singleton-like para chamadas a Riot Games API.
    Controla rate limit via sleep entre requests.
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_api_key()
        if not self.api_key:
            raise ValueError(
                "RIOT_DEVELOPER_API nao encontrada. "
                "Verifique o .env ou a variavel de ambiente."
            )

        self.session = http_requests.Session()
        self.session.headers.update({
            "X-Riot-Token": self.api_key,
            "Accept": "application/json",
        })

        self._last_request_time = 0.0

    def _enforce_rate_limit(self):
        """
        Garante que nao excedemos o rate limit.
        Estrategia conservadora: 1 request a cada SAFE_DELAY_BETWEEN_REQUESTS.
        Isso da ~46 req/min, bem abaixo dos 100/2min.
        Usa time.monotonic() para ser imune a ajustes de clock (ex: NTP sync no Docker/WSL2).
        """
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if self._last_request_time > 0 and elapsed < SAFE_DELAY_BETWEEN_REQUESTS:
            sleep_time = SAFE_DELAY_BETWEEN_REQUESTS - elapsed
            time.sleep(sleep_time)
        self._last_request_time = time.monotonic()

    def get(self, url: str, params: dict = None) -> dict | list | None:
        """
        Faz GET na API com rate limiting e retry.

        Retorna:
            - dict/list com o JSON de resposta em caso de sucesso
            - None se esgotou retries ou erro irrecuperavel (ex: 403, 404)
        """
        for attempt in range(1, MAX_RETRIES + 1):
            self._enforce_rate_limit()

            try:
                response = self.session.get(url, params=params, timeout=15)
            except http_requests.exceptions.RequestException as exc:
                logger.error(
                    "Erro de rede na tentativa %d/%d para %s: %s",
                    attempt, MAX_RETRIES, url, exc
                )
                self._backoff(attempt)
                continue

            status = response.status_code

            # Sucesso
            if status == 200:
                return response.json()

            # Rate limited - respeitar Retry-After
            if status == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logger.warning(
                    "Rate limited (429). Retry-After: %ds. Tentativa %d/%d. URL: %s",
                    retry_after, attempt, MAX_RETRIES, url
                )
                time.sleep(retry_after + 1)
                continue

            # Erros do cliente que nao adianta retry
            if status in (400, 401, 403):
                logger.error(
                    "Erro %d (irrecuperavel) para %s. Body: %s",
                    status, url, response.text[:200]
                )
                return None

            # 404 - recurso nao existe (jogador mudou de nome, etc)
            if status == 404:
                logger.warning("404 Not Found para %s", url)
                return None

            # Erros de servidor (5xx) - retry com backoff
            if 500 <= status < 600:
                logger.warning(
                    "Erro %d do servidor. Tentativa %d/%d. URL: %s",
                    status, attempt, MAX_RETRIES, url
                )
                self._backoff(attempt)
                continue

            # Qualquer outro status inesperado
            logger.error(
                "Status inesperado %d para %s. Body: %s",
                status, url, response.text[:200]
            )
            return None

        logger.error("Esgotou %d tentativas para %s", MAX_RETRIES, url)
        return None

    def health_check(self) -> bool:
        """
        Verifica se a API key esta funcional fazendo uma chamada leve.
        Usa o endpoint de status do Riot (Account-V1 com dados conhecidos).
        Retorna True se a key esta valida, False caso contrario.
        """
        url = "https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/Faker/KR1"
        self._enforce_rate_limit()
        try:
            response = self.session.get(url, timeout=10)
        except http_requests.exceptions.RequestException as exc:
            logger.error("Health check falhou (erro de rede): %s", exc)
            return False

        if response.status_code == 200:
            logger.info("Health check OK. API key valida.")
            return True

        if response.status_code in (401, 403):
            logger.critical(
                "Health check FALHOU. API key invalida ou expirada (HTTP %d).",
                response.status_code
            )
            return False

        logger.warning(
            "Health check retornou status inesperado: %d", response.status_code
        )
        return True

    def close(self):
        """Fecha a session HTTP."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _backoff(self, attempt: int):
        """Exponential backoff: 2^attempt segundos."""
        wait = RETRY_BACKOFF_BASE ** attempt
        logger.info("Backoff: aguardando %ds antes da proxima tentativa.", wait)
        time.sleep(wait)
