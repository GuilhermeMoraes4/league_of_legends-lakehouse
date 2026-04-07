"""
Upload dos dados bronze para Databricks e carregamento em Delta tables.

Fluxo em 2 fases:
1. Upload dos JSONs locais para UC Volumes via Files API (staging)
2. Execucao de SQL no SQL Warehouse para carregar dos Volumes em Delta tables

Destino final:
  loldata.cblol_bronze.accounts      (raw_json STRING, extraction_date DATE)
  loldata.cblol_bronze.match_ids     (raw_json STRING, extraction_date DATE)
  loldata.cblol_bronze.matches       (raw_json STRING, extraction_date DATE)
  loldata.cblol_bronze.timelines     (raw_json STRING, extraction_date DATE)
"""

import logging
import os

from src.extract.config import BASE_OUTPUT_DIR
from src.upload.config import (
    BRONZE_TABLES,
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    DBFS_BRONZE_PATH,
    get_databricks_host,
    get_databricks_token,
    get_databricks_warehouse_id,
)
from src.upload.dbfs_client import DBFSClient

logger = logging.getLogger(__name__)


def upload_bronze(execution_date):
    """
    Fase 1: Upload dos JSONs para UC Volumes (staging).
    Fase 2: SQL no Warehouse para carregar em Delta tables.

    Retorna tupla (arquivos_enviados, tabelas_carregadas).
    """
    host = get_databricks_host()
    token = get_databricks_token()

    if not host or not token:
        logger.warning(
            "DATABRICKS_HOST ou DATABRICKS_TOKEN nao configurados. "
            "Upload ignorado."
        )
        return 0, 0

    uploaded = _upload_to_volumes(host, token, execution_date)
    tables_loaded = _load_to_tables(host, token, execution_date)

    return uploaded, tables_loaded


def _upload_to_volumes(host, token, execution_date):
    """Faz upload dos JSONs locais para UC Volumes."""
    uploaded = 0
    skipped = 0

    with DBFSClient(host, token) as client:
        for subdir in BRONZE_TABLES:
            local_dir = os.path.join(BASE_OUTPUT_DIR, subdir, f"dt={execution_date}")

            if not os.path.isdir(local_dir):
                logger.info("Sem dados para %s/%s, pulando.", subdir, f"dt={execution_date}")
                continue

            dbfs_dir = f"{DBFS_BRONZE_PATH}/{subdir}/dt={execution_date}"

            files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
            logger.info("[Volumes] %s: %d arquivos", subdir, len(files))

            for filename in files:
                local_path = os.path.join(local_dir, filename)
                dbfs_path = f"{dbfs_dir}/{filename}"

                if client.file_exists(dbfs_path):
                    skipped += 1
                    continue

                if client.put_file(local_path, dbfs_path):
                    uploaded += 1
                else:
                    logger.error("Falha no upload: %s", dbfs_path)

    logger.info(
        "[Volumes] Upload finalizado. Enviados: %d | Ja existiam: %d",
        uploaded, skipped,
    )
    return uploaded


def _load_to_tables(host, token, execution_date):
    """Carrega dados dos Volumes em Delta tables via SQL Warehouse."""
    warehouse_id = get_databricks_warehouse_id()

    if not warehouse_id:
        logger.warning(
            "DATABRICKS_WAREHOUSE_ID nao configurado. "
            "Dados ficam apenas nos Volumes."
        )
        return 0

    from src.upload.databricks_sql import DatabricksSQLClient

    tables_loaded = 0

    try:
        with DatabricksSQLClient(warehouse_id, host, token) as client:
            if not client.health_check():
                logger.error("SQL Warehouse nao acessivel.")
                return 0

            _ensure_schema(client)

            for table_name in BRONZE_TABLES:
                local_dir = os.path.join(BASE_OUTPUT_DIR, table_name, f"dt={execution_date}")
                if not os.path.isdir(local_dir):
                    logger.info("[Tables] Sem dados locais para %s, pulando.", table_name)
                    continue

                volume_path = f"/{DBFS_BRONZE_PATH}/{table_name}/dt={execution_date}"
                full_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{table_name}"

                if _load_table(client, table_name, volume_path, full_table, execution_date):
                    tables_loaded += 1

    except Exception as exc:
        logger.error("Erro ao carregar tabelas: %s", exc)

    logger.info("[Tables] Tabelas carregadas: %d de %d", tables_loaded, len(BRONZE_TABLES))
    return tables_loaded


def _ensure_schema(client):
    """Cria catalog e schema se nao existirem."""
    client.execute(f"CREATE CATALOG IF NOT EXISTS {DATABRICKS_CATALOG}")
    client.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}")
    logger.info("Schema %s.%s verificado.", DATABRICKS_CATALOG, DATABRICKS_SCHEMA)


def _load_table(client, table_name, volume_path, full_table, execution_date):
    """Carrega dados de um diretorio do Volume em uma Delta table."""
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            raw_json STRING,
            extraction_date DATE
        ) USING DELTA
    """
    result = client.execute(create_sql)
    if not result:
        logger.error("Falha ao criar tabela %s.", full_table)
        return False

    insert_sql = f"""
        INSERT INTO {full_table} (raw_json, extraction_date)
        SELECT
            value AS raw_json,
            DATE '{execution_date}' AS extraction_date
        FROM read_files(
            '{volume_path}/*.json',
            format => 'text',
            wholeText => true
        )
    """
    result = client.execute(insert_sql, wait_timeout="50s")
    if result:
        logger.info("[Tables] %s carregada com sucesso.", full_table)
        return True

    logger.error("[Tables] Falha ao carregar %s.", full_table)
    return False
