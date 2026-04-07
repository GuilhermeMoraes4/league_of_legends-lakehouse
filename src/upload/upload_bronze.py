"""
Upload dos dados bronze (local) para o DBFS do Databricks Community Edition.

Espelha a estrutura de diretorios local no DBFS:
  data/bronze/accounts/dt=2026-04-04/  ->  /lol-lakehouse/bronze/accounts/dt=2026-04-04/
  data/bronze/matches/dt=2026-04-04/   ->  /lol-lakehouse/bronze/matches/dt=2026-04-04/
  ...

Idempotente: pula arquivos que ja existem no DBFS.
"""

import logging
import os

from src.extract.config import BASE_OUTPUT_DIR
from src.upload.config import DBFS_BRONZE_PATH, get_databricks_host, get_databricks_token
from src.upload.dbfs_client import DBFSClient

logger = logging.getLogger(__name__)

BRONZE_SUBDIRS = ["accounts", "match_ids", "matches", "timelines"]


def upload_bronze(execution_date):
    """
    Faz upload dos dados bronze de uma data especifica para o DBFS.

    Retorna o numero de arquivos enviados (0 se Databricks nao configurado).
    """
    host = get_databricks_host()
    token = get_databricks_token()

    if not host or not token:
        logger.warning(
            "DATABRICKS_HOST ou DATABRICKS_TOKEN nao configurados. "
            "Upload para DBFS ignorado."
        )
        return 0

    uploaded = 0
    skipped = 0
    failed = 0

    with DBFSClient(host, token) as client:
        for subdir in BRONZE_SUBDIRS:
            local_dir = os.path.join(BASE_OUTPUT_DIR, subdir, f"dt={execution_date}")

            if not os.path.isdir(local_dir):
                logger.info("Diretorio nao existe, pulando: %s/%s", subdir, f"dt={execution_date}")
                continue

            dbfs_dir = f"{DBFS_BRONZE_PATH}/{subdir}/dt={execution_date}"
            client.mkdirs(dbfs_dir)

            files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
            logger.info("Processando %s: %d arquivos", subdir, len(files))

            for filename in files:
                local_path = os.path.join(local_dir, filename)
                dbfs_path = f"{dbfs_dir}/{filename}"

                if client.file_exists(dbfs_path):
                    skipped += 1
                    continue

                if client.put_file(local_path, dbfs_path):
                    uploaded += 1
                else:
                    failed += 1
                    logger.error("Falha no upload: %s -> %s", local_path, dbfs_path)

    logger.info(
        "Upload para DBFS finalizado. Enviados: %d | Ja existiam: %d | Falha: %d",
        uploaded, skipped, failed,
    )
    return uploaded
