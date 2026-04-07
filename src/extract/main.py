"""
Orquestrador principal da extracao.
Chamado pelo Airflow via TaskFlow API ou diretamente via CLI.

Pipeline:
1. Resolve Riot IDs -> PUUIDs (Account-V1)
2. Coleta match IDs de cada jogador (Match-V5)
3. Baixa detalhes de cada partida (Match-V5)
4. Salva tudo como JSON raw no bronze layer

Uso:
    python -m src.extract.main all --date 2026-03-26
    python -m src.extract.main accounts --date 2026-03-26
    python -m src.extract.main match-ids --date 2026-03-26
    python -m src.extract.main match-details --date 2026-03-26 --timeline
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from src.extract.config import BASE_OUTPUT_DIR, get_api_key
from src.extract.extract_accounts import extract_accounts, load_accounts
from src.extract.extract_matches import extract_match_details, extract_match_ids, load_match_ids
from src.extract.riot_api_client import RiotAPIClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("riot_ingestion")


def _validate_and_create_client():
    """Valida a API key e retorna um RiotAPIClient pronto."""
    api_key = get_api_key()

    if not api_key:
        logger.critical(
            "RIOT_DEVELOPER_API nao definida. "
            "Atualize o .env com uma key valida do developer.riotgames.com"
        )
        sys.exit(1)

    if not api_key.startswith("RGAPI-"):
        logger.critical(
            "RIOT_DEVELOPER_API com formato invalido. "
            "Deve comecar com 'RGAPI-'."
        )
        sys.exit(1)

    client = RiotAPIClient()

    if not client.health_check():
        logger.critical(
            "API key expirada ou invalida. "
            "Regenere em developer.riotgames.com e atualize o .env."
        )
        client.close()
        sys.exit(1)

    return client


def step_accounts(execution_date, client=None):
    """STEP 1: Resolver Riot IDs -> PUUIDs."""
    logger.info("[STEP 1] Extraindo contas (Account-V1)...")
    owns_client = client is None
    if owns_client:
        client = _validate_and_create_client()

    try:
        accounts = extract_accounts(client, execution_date)
    finally:
        if owns_client:
            client.close()

    if not accounts:
        logger.critical(
            "Nenhuma conta extraida com sucesso. "
            "Verifique a API key (expira a cada 24h) e os Riot IDs no config.py."
        )
        sys.exit(1)

    logger.info("Contas extraidas: %d", len(accounts))
    return accounts


def step_match_ids(execution_date, client=None):
    """STEP 2: Coletar match IDs de cada jogador."""
    logger.info("[STEP 2] Coletando match IDs (Match-V5)...")

    accounts = load_accounts(execution_date)
    if not accounts:
        logger.critical(
            "Nenhuma conta encontrada para %s. "
            "Execute o step 'accounts' antes.", execution_date
        )
        sys.exit(1)

    owns_client = client is None
    if owns_client:
        client = _validate_and_create_client()

    try:
        match_ids = extract_match_ids(client, accounts, execution_date)
    finally:
        if owns_client:
            client.close()

    if not match_ids:
        logger.warning(
            "Nenhum match ID encontrado. "
            "Os jogadores podem nao ter jogado ranked recentemente."
        )

    logger.info("Match IDs unicos coletados: %d", len(match_ids))
    return match_ids


def step_match_details(execution_date, include_timeline=False, client=None):
    """STEP 3: Baixar detalhes de cada partida."""
    logger.info("[STEP 3] Extraindo detalhes das partidas (Match-V5)...")

    match_ids = load_match_ids(execution_date)
    if not match_ids:
        logger.warning(
            "Nenhum match ID encontrado para %s. "
            "Execute o step 'match-ids' antes ou nao ha partidas.", execution_date
        )
        return 0

    owns_client = client is None
    if owns_client:
        client = _validate_and_create_client()

    try:
        extracted = extract_match_details(
            client, match_ids, execution_date, include_timeline
        )
    finally:
        if owns_client:
            client.close()

    logger.info("Partidas extraidas nesta execucao: %d", extracted)
    return extracted


def run_pipeline(execution_date, include_timeline=False):
    """Executa o pipeline completo com um unico client (1 health check, nao 3)."""
    logger.info("=" * 70)
    logger.info("INICIO DO PIPELINE DE EXTRACAO - RIOT GAMES API")
    logger.info("Data de execucao: %s", execution_date)
    logger.info("Output dir: %s", BASE_OUTPUT_DIR)
    logger.info("Timeline: %s", include_timeline)
    logger.info("=" * 70)

    with _validate_and_create_client() as client:
        step_accounts(execution_date, client)
        step_match_ids(execution_date, client)
        step_match_details(execution_date, include_timeline, client)

    logger.info("=" * 70)
    logger.info("PIPELINE FINALIZADO")
    logger.info("Dados salvos em: %s", BASE_OUTPUT_DIR)
    logger.info("=" * 70)


def main():
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Pipeline de extracao da Riot Games API para o Lakehouse."
    )
    parser.add_argument(
        "step",
        choices=["all", "accounts", "match-ids", "match-details"],
        nargs="?",
        default="all",
        help="Step a executar. Default: all (pipeline completo).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Data de execucao (YYYY-MM-DD). Default: hoje (UTC).",
    )
    parser.add_argument(
        "--timeline",
        action="store_true",
        default=False,
        help="Incluir extracao de timelines (dobra requests).",
    )

    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error("Formato de data invalido: %s. Use YYYY-MM-DD.", args.date)
        sys.exit(1)

    if args.step == "all":
        run_pipeline(args.date, args.timeline)
    elif args.step == "accounts":
        step_accounts(args.date)
    elif args.step == "match-ids":
        step_match_ids(args.date)
    elif args.step == "match-details":
        step_match_details(args.date, args.timeline)


if __name__ == "__main__":
    main()
