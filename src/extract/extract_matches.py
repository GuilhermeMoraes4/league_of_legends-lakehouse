"""
Extracao de partidas via Match-V5.

Endpoints:
- GET /lol/match/v5/matches/by-puuid/{puuid}/ids  (lista de match IDs)
- GET /lol/match/v5/matches/{matchId}              (detalhes da partida)
- GET /lol/match/v5/matches/{matchId}/timeline      (timeline - opcional)

Host: americas.api.riotgames.com (regional)

A logica eh:
1. Para cada PUUID, buscar os ultimos N match IDs (ranked solo queue)
2. Deduplicar match IDs (jogadores do mesmo time compartilham partidas)
3. Para cada match ID unico, buscar os detalhes completos
4. Salvar tudo como JSON no bronze layer
"""

import json
import logging
import os
from datetime import datetime, timezone

from src.extract.config import (
    BASE_OUTPUT_DIR,
    MATCHES_PER_PLAYER,
    QUEUE_RANKED_SOLO,
    REGIONAL_URL,
)
from src.extract.riot_api_client import RiotAPIClient

logger = logging.getLogger(__name__)


def extract_match_ids(
    client: RiotAPIClient,
    accounts: list[dict],
    execution_date: str = None,
) -> list[str]:
    """
    Coleta match IDs de todos os jogadores e retorna lista deduplicada.

    Args:
        client: instancia do RiotAPIClient
        accounts: lista de dicts com 'puuid' (output de extract_accounts)
        execution_date: data de execucao YYYY-MM-DD

    Returns:
        Lista unica de match IDs
    """
    if execution_date is None:
        execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    all_match_ids = set()
    player_match_map = {}

    logger.info(
        "Buscando match IDs para %d jogadores (queue=%d, count=%d).",
        len(accounts), QUEUE_RANKED_SOLO, MATCHES_PER_PLAYER
    )

    for account in accounts:
        puuid = account["puuid"]
        player_label = f"{account['team']}/{account['game_name']}"

        url = f"{REGIONAL_URL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {
            "queue": QUEUE_RANKED_SOLO,
            "type": "ranked",
            "start": 0,
            "count": MATCHES_PER_PLAYER,
        }

        match_ids = client.get(url, params=params)

        if match_ids and isinstance(match_ids, list):
            player_match_map[player_label] = match_ids
            before_count = len(all_match_ids)
            all_match_ids.update(match_ids)
            new_count = len(all_match_ids) - before_count
            logger.info(
                "[OK] %s -> %d partidas (%d novas)",
                player_label, len(match_ids), new_count
            )
        else:
            logger.warning(
                "[FAIL] %s -> Nenhuma partida retornada.",
                player_label
            )

    unique_ids = sorted(all_match_ids)

    # Salvar mapeamento jogador->partidas no bronze
    _save_match_ids(unique_ids, player_match_map, execution_date)

    logger.info(
        "Total de match IDs unicos coletados: %d",
        len(unique_ids)
    )

    return unique_ids


def extract_match_details(
    client: RiotAPIClient,
    match_ids: list[str],
    execution_date: str = None,
    include_timeline: bool = False,
) -> int:
    """
    Para cada match ID, busca os detalhes completos e salva no bronze layer.
    Pula partidas que ja foram extraidas (idempotencia).

    Args:
        client: instancia do RiotAPIClient
        match_ids: lista de match IDs para extrair
        execution_date: data de execucao YYYY-MM-DD
        include_timeline: se True, tambem extrai a timeline (mais requests)

    Returns:
        Quantidade de partidas extraidas com sucesso
    """
    if execution_date is None:
        execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    matches_dir = os.path.join(BASE_OUTPUT_DIR, "matches", f"dt={execution_date}")
    timelines_dir = os.path.join(BASE_OUTPUT_DIR, "timelines", f"dt={execution_date}")
    os.makedirs(matches_dir, exist_ok=True)
    if include_timeline:
        os.makedirs(timelines_dir, exist_ok=True)

    success_count = 0
    skip_count = 0
    fail_count = 0

    total = len(match_ids)
    logger.info(
        "Iniciando extracao de detalhes para %d partidas (timeline=%s).",
        total, include_timeline
    )

    for idx, match_id in enumerate(match_ids, 1):
        # Idempotencia: pular se ja extraido
        match_path = os.path.join(matches_dir, f"{match_id}.json")
        if os.path.exists(match_path):
            skip_count += 1
            continue

        # Match details
        url = f"{REGIONAL_URL}/lol/match/v5/matches/{match_id}"
        match_data = client.get(url)

        if match_data:
            # Enriquecer com metadata de extracao
            match_data["_extraction_metadata"] = {
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "execution_date": execution_date,
                "source": "riot_api_match_v5",
            }

            with open(match_path, "w", encoding="utf-8") as f:
                json.dump(match_data, f, ensure_ascii=False)

            success_count += 1

            if idx % 10 == 0 or idx == total:
                logger.info(
                    "Progresso: %d/%d (sucesso=%d, skip=%d, falha=%d)",
                    idx, total, success_count, skip_count, fail_count
                )
        else:
            fail_count += 1
            logger.warning(
                "Falha ao extrair match %s (%d/%d)",
                match_id, idx, total
            )
            continue

        # Timeline (opcional - consome requests extras)
        if include_timeline:
            timeline_path = os.path.join(timelines_dir, f"{match_id}_timeline.json")
            if not os.path.exists(timeline_path):
                timeline_url = f"{REGIONAL_URL}/lol/match/v5/matches/{match_id}/timeline"
                timeline_data = client.get(timeline_url)
                if timeline_data:
                    with open(timeline_path, "w", encoding="utf-8") as f:
                        json.dump(timeline_data, f, ensure_ascii=False)

    logger.info(
        "Extracao de detalhes finalizada. "
        "Sucesso: %d | Ja existiam: %d | Falha: %d",
        success_count, skip_count, fail_count
    )

    return success_count


def _save_match_ids(
    unique_ids: list[str],
    player_match_map: dict,
    execution_date: str,
):
    """Salva os match IDs e o mapeamento jogador->partidas."""
    output_dir = os.path.join(BASE_OUTPUT_DIR, "match_ids", f"dt={execution_date}")
    os.makedirs(output_dir, exist_ok=True)

    # Lista unica de IDs
    ids_path = os.path.join(output_dir, "match_ids.json")
    with open(ids_path, "w", encoding="utf-8") as f:
        json.dump(unique_ids, f, ensure_ascii=False, indent=2)

    # Mapeamento jogador -> partidas (util pra analytics)
    map_path = os.path.join(output_dir, "player_match_map.json")
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(player_match_map, f, ensure_ascii=False, indent=2)

    logger.info("Match IDs salvos em: %s", output_dir)


def load_match_ids(execution_date):
    """Carrega match IDs previamente extraidos do bronze layer."""
    path = os.path.join(
        BASE_OUTPUT_DIR, "match_ids", f"dt={execution_date}", "match_ids.json"
    )

    if not os.path.exists(path):
        logger.warning("Arquivo de match IDs nao encontrado: %s", path)
        return []

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
