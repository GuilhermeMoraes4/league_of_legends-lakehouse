"""
Cliente para execucao de SQL no Databricks via SQL Statement Execution API.

Funciona com SQL Warehouses (inclusive Serverless Starter Warehouse).
Nao requer cluster all-purpose rodando.

Endpoint: POST /api/2.0/sql/statements/
"""

import logging
import time

import requests as http_requests

from src.upload.config import get_databricks_host, get_databricks_token

logger = logging.getLogger(__name__)

POLL_INTERVAL = 2
POLL_TIMEOUT = 300


class DatabricksSQLClient:
    """Executa SQL no Databricks via SQL Statement Execution API."""

    def __init__(self, warehouse_id, host=None, token=None):
        self.warehouse_id = warehouse_id
        self.host = host or get_databricks_host()
        self.token = token or get_databricks_token()
        self.base_url = f"https://{self.host}/api/2.0/sql/statements"
        self.session = http_requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        })

    def execute(self, statement, wait_timeout="30s"):
        """
        Executa um SQL statement e retorna o resultado.

        Args:
            statement: comando SQL
            wait_timeout: tempo maximo de espera inline (default 30s)

        Returns:
            dict com status e resultado, ou None em caso de erro
        """
        resp = self.session.post(
            self.base_url,
            json={
                "warehouse_id": self.warehouse_id,
                "statement": statement,
                "wait_timeout": wait_timeout,
            },
            timeout=60,
        )

        if resp.status_code not in (200, 202):
            logger.error(
                "Falha ao executar SQL (HTTP %d): %s",
                resp.status_code, resp.text[:500],
            )
            return None

        data = resp.json()
        status = data.get("status", {}).get("state", "")

        if status == "SUCCEEDED":
            return data

        if status == "PENDING" or status == "RUNNING":
            statement_id = data.get("statement_id")
            return self._poll_statement(statement_id)

        if status == "FAILED":
            error = data.get("status", {}).get("error", {})
            logger.error("SQL falhou: %s", error.get("message", "erro desconhecido"))
            return None

        logger.error("Status inesperado: %s", status)
        return None

    def _poll_statement(self, statement_id):
        """Faz polling ate o statement terminar."""
        elapsed = 0
        while elapsed < POLL_TIMEOUT:
            resp = self.session.get(
                f"{self.base_url}/{statement_id}",
                timeout=10,
            )

            if resp.status_code != 200:
                time.sleep(POLL_INTERVAL)
                elapsed += POLL_INTERVAL
                continue

            data = resp.json()
            status = data.get("status", {}).get("state", "")

            if status == "SUCCEEDED":
                return data
            if status == "FAILED":
                error = data.get("status", {}).get("error", {})
                logger.error("SQL falhou: %s", error.get("message", ""))
                return None
            if status in ("CANCELED", "CLOSED"):
                logger.error("SQL cancelado/fechado.")
                return None

            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        logger.error("Timeout esperando SQL finalizar (statement_id=%s).", statement_id)
        return None

    def health_check(self):
        """Verifica se o warehouse esta acessivel."""
        result = self.execute("SELECT 1 AS ok", wait_timeout="10s")
        if result:
            logger.info("Health check SQL Warehouse OK.")
            return True
        logger.error("Health check SQL Warehouse falhou.")
        return False

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False
