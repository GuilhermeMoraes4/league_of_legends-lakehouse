"""
Extracao de contas via Account-V1.

Endpoint: GET /riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}
Host: americas.api.riotgames.com (regional)

Retorna o PUUID de cada jogador, que eh a chave universal pra todos os outros endpoints.
"""

import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import quote

from src.extract.config import BASE_OUTPUT_DIR, CBLOL_PLAYERS, REGIONAL_URL
from src.extract.riot_api_client import RiotAPIClient

logger = logging.getLogger(__name__)


def extract_accounts(client: RiotAPIClient, execution_date: str = None) -> list[dict]:
    """
    Para cada jogador em CBLOL_PLAYERS, resolve o Riot ID para PUUID.

    Args:
        client: instancia do RiotAPIClient
        execution_date: data de execucao no formato YYYY-MM-DD (para particionar o output)

    Returns:
        Lista de dicts com os dados enriquecidos (player info + puuid)
    """
    if execution_date is None:
        execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results = []
    failed = []

    logger.info(
        "Iniciando extracao de contas para %d jogadores.",
        len(CBLOL_PLAYERS)
    )

    for player in CBLOL_PLAYERS:
        game_name = player["game_name"]
        tag_line = player["tag_line"]

        url = (
            f"{REGIONAL_URL}/riot/account/v1/accounts"
            f"/by-riot-id/{quote(game_name)}/{quote(tag_line)}"
        )

        data = client.get(url)

        if data and "puuid" in data:
            enriched = {
                **player,
                "puuid": data["puuid"],
                "game_name_api": data.get("gameName", game_name),
                "tag_line_api": data.get("tagLine", tag_line),
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(enriched)
            logger.info(
                "[OK] %s | %s#%s -> puuid: %s...%s",
                player["team"], game_name, tag_line,
                data["puuid"][:8], data["puuid"][-4:]
            )
        else:
            failed.append(player)
            logger.warning(
                "[FAIL] %s | %s#%s -> Nao encontrado ou erro na API.",
                player["team"], game_name, tag_line
            )

    # Persistir no bronze layer
    _save_accounts(results, execution_date)

    logger.info(
        "Extracao de contas finalizada. Sucesso: %d | Falha: %d",
        len(results), len(failed)
    )

    if failed:
        logger.warning(
            "Jogadores que falharam (verifique Riot ID/tagLine): %s",
            json.dumps(
                [f"{p['team']}/{p['game_name']}#{p['tag_line']}" for p in failed],
                ensure_ascii=False
            )
        )

    return results


def _save_accounts(accounts: list[dict], execution_date: str):
    """Salva os dados de contas no bronze layer como JSON particionado por data."""
    output_dir = os.path.join(BASE_OUTPUT_DIR, "accounts", f"dt={execution_date}")
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "accounts.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)

    logger.info("Contas salvas em: %s (%d registros)", output_path, len(accounts))


def load_accounts(execution_date: str = None) -> list[dict]:
    """
    Carrega as contas previamente extraidas do bronze layer.
    Util pra nao precisar re-extrair PUUIDs toda vez.
    """
    if execution_date is None:
        execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    path = os.path.join(
        BASE_OUTPUT_DIR, "accounts", f"dt={execution_date}", "accounts.json"
    )

    if not os.path.exists(path):
        logger.warning("Arquivo de contas nao encontrado: %s", path)
        return []

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
