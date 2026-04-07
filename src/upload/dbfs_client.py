"""
Cliente HTTP para upload no Databricks via Unity Catalog Volumes (Files API).

Migrado de DBFS API (desabilitada no Community Edition) para Files API 2.0.
Endpoint: PUT /api/2.0/fs/files/{path}

Responsabilidades:
- Upload de arquivos locais para UC Volumes
- Verificacao de existencia de arquivos (idempotencia)
- Retry com exponential backoff
"""

import logging
import os
import time

import requests as http_requests

from src.upload.config import (
    get_databricks_host,
    get_databricks_token,
)

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2


class DBFSClient:
    """
    Client para upload via Databricks Files API 2.0 (Unity Catalog Volumes).
    Funciona com Databricks Community Edition sem cluster ativo.
    """

    def __init__(self, host=None, token=None):
        self.host = host or get_databricks_host()
        self.token = token or get_databricks_token()

        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST e DATABRICKS_TOKEN precisam estar configurados. "
                "Gere um PAT em Databricks > Settings > Developer > Access tokens."
            )

        self.base_url = f"https://{self.host}/api/2.0/fs/files"
        self.session = http_requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
        })

    def mkdirs(self, path):
        """Noop — Files API cria diretorios automaticamente no upload."""
        return True

    def put_file(self, local_path, remote_path):
        """Upload de arquivo local para UC Volume via Files API PUT."""
        url = f"{self.base_url}/{remote_path.lstrip('/')}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with open(local_path, "rb") as f:
                    resp = self.session.put(
                        url,
                        data=f,
                        headers={"Content-Type": "application/octet-stream"},
                        timeout=60,
                    )
            except http_requests.exceptions.RequestException as exc:
                logger.error(
                    "Erro de rede em upload (tentativa %d/%d): %s",
                    attempt, MAX_RETRIES, exc,
                )
                self._backoff(attempt)
                continue

            if resp.status_code in (200, 201, 204):
                return True

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 10))
                logger.warning("Rate limited. Retry-After: %ds", retry_after)
                time.sleep(retry_after + 1)
                continue

            if resp.status_code in (401, 403):
                logger.error(
                    "Auth falhou (HTTP %d). Verifique o DATABRICKS_TOKEN.",
                    resp.status_code,
                )
                return False

            if 500 <= resp.status_code < 600:
                logger.warning(
                    "Erro %d no servidor (tentativa %d/%d)",
                    resp.status_code, attempt, MAX_RETRIES,
                )
                self._backoff(attempt)
                continue

            logger.error(
                "Resposta inesperada: %d — %s",
                resp.status_code, resp.text[:300],
            )
            return False

        logger.error("Esgotou %d tentativas para upload de %s", MAX_RETRIES, remote_path)
        return False

    def get_status(self, path):
        """Verifica se arquivo existe via HEAD request na Files API."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            resp = self.session.head(url, timeout=10)
            if resp.status_code == 200:
                return {"path": path, "is_dir": False}
            return None
        except http_requests.exceptions.RequestException as exc:
            logger.error("Erro ao verificar %s: %s", path, exc)
            return None

    def file_exists(self, path):
        """Verifica se um arquivo existe no UC Volume."""
        return self.get_status(path) is not None

    def health_check(self):
        """Verifica se o token esta valido listando volumes."""
        url = f"https://{self.host}/api/2.1/unity-catalog/volumes?catalog_name=workspace&schema_name=default"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                logger.info("Health check Databricks OK.")
                return True
            logger.error("Health check falhou (HTTP %d).", resp.status_code)
            return False
        except http_requests.exceptions.RequestException as exc:
            logger.error("Health check falhou (rede): %s", exc)
            return False

    def _backoff(self, attempt):
        wait = RETRY_BACKOFF_BASE ** attempt
        logger.info("Backoff: aguardando %ds.", wait)
        time.sleep(wait)

    def close(self):
        """Fecha a session HTTP."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False
