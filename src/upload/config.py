"""
Configuracoes do upload para Databricks Community Edition (DBFS).
"""

import os


def get_databricks_host():
    """Retorna o host do Databricks (lazy, sem side effect no import)."""
    return os.getenv("DATABRICKS_HOST", "")


def get_databricks_token():
    """Retorna o PAT do Databricks (lazy, sem side effect no import)."""
    return os.getenv("DATABRICKS_TOKEN", "")


# Path base no UC Volume onde os dados bronze serao salvos
# Formato: Volumes/{catalog}/{schema}/{volume}
DBFS_BRONZE_PATH = os.getenv("DBFS_BRONZE_PATH", "Volumes/workspace/default/bronze")

# Tamanho maximo para upload direto via PUT (1MB)
DBFS_PUT_MAX_BYTES = 1 * 1024 * 1024
