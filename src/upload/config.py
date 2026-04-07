"""
Configuracoes do upload para Databricks Community Edition.
"""

import os


def get_databricks_host():
    """Retorna o host do Databricks (lazy, sem side effect no import)."""
    return os.getenv("DATABRICKS_HOST", "")


def get_databricks_token():
    """Retorna o PAT do Databricks (lazy, sem side effect no import)."""
    return os.getenv("DATABRICKS_TOKEN", "")


def get_databricks_warehouse_id():
    """Retorna o warehouse ID do SQL Warehouse (lazy, sem side effect no import)."""
    return os.getenv("DATABRICKS_WAREHOUSE_ID", "")


# Path base no UC Volume onde os dados bronze sao staging
# Formato: Volumes/{catalog}/{schema}/{volume}
DBFS_BRONZE_PATH = os.getenv("DBFS_BRONZE_PATH", "Volumes/workspace/default/bronze")

# Tamanho maximo para upload direto via PUT (1MB)
DBFS_PUT_MAX_BYTES = 1 * 1024 * 1024

# Catalog e schema para as Delta tables no bronze
DATABRICKS_CATALOG = "loldata"
DATABRICKS_SCHEMA = "cblol_bronze"

# Tabelas bronze (mapeiam para os subdiretorios do bronze layer)
BRONZE_TABLES = ["accounts", "match_ids", "matches", "timelines"]
