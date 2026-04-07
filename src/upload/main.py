"""
Entry point para upload e carregamento de dados bronze no Databricks.
Chamado pelo Airflow via TaskFlow API ou diretamente via CLI.

Uso:
    python -m src.upload.main --date 2026-04-07
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from src.upload.upload_bronze import upload_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("databricks_loader")


def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Upload e carregamento de dados bronze no Databricks."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Data de execucao (YYYY-MM-DD). Default: hoje (UTC).",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error("Formato de data invalido: %s. Use YYYY-MM-DD.", args.date)
        sys.exit(1)

    logger.info("Iniciando upload e carregamento — data: %s", args.date)
    uploaded, tables = upload_bronze(args.date)
    logger.info(
        "Concluido. Arquivos enviados: %d | Tabelas carregadas: %d",
        uploaded, tables,
    )


if __name__ == "__main__":
    main()
