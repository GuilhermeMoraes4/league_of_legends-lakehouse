"""
Configuracoes centrais do pipeline de extracao da Riot Games API.
Contém endpoints, rate limits, lista de jogadores CBLOL 2026, e constantes.
"""

import os

# ---------------------------------------------------------------------------
# Riot Games API
# ---------------------------------------------------------------------------


def get_api_key():
    """Retorna a API key da variavel de ambiente (lazy, sem side effect no import)."""
    return os.getenv("RIOT_DEVELOPER_API")

# Plataforma BR1 (summoner-v4, league-v4, spectator-v5)
PLATFORM_URL = "https://br1.api.riotgames.com"

# Regional Americas (account-v1, match-v5)
REGIONAL_URL = "https://americas.api.riotgames.com"

# Rate limits da dev key
RATE_LIMIT_PER_SECOND = 20
RATE_LIMIT_PER_2MIN = 100

# Delay conservador entre requests (~46/min, bem abaixo do limite)
SAFE_DELAY_BETWEEN_REQUESTS = 1.3

# Retry config
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # exponential backoff: 2^attempt seconds

# Match-V5: quantas partidas buscar por jogador (max 100 por chamada)
MATCHES_PER_PLAYER = 20

# Queue IDs relevantes
QUEUE_RANKED_SOLO = 420

# ---------------------------------------------------------------------------
# Output paths (bronze layer - raw JSON)
# ---------------------------------------------------------------------------

# Dentro do container Airflow: /opt/airflow/data/bronze/
# Localmente: ./data/bronze/
BASE_OUTPUT_DIR = os.getenv("BRONZE_OUTPUT_DIR", "/opt/airflow/data/bronze")

# ---------------------------------------------------------------------------
# CBLOL 2026 - Rosters
# ---------------------------------------------------------------------------
# gameName e tagLine sao usados no endpoint Account-V1.
# tagLine "BR1" eh o padrao pra contas brasileiras que nunca mudaram.
# Jogadores coreanos podem ter tagLine diferente (ex: "KR1", custom).
#
# IMPORTANTE: Esses Riot IDs precisam ser validados manualmente.
# Se um jogador mudou o nome ou tagLine, a API retorna 404.
# Atualize conforme necessario.
# ---------------------------------------------------------------------------

CBLOL_PLAYERS = [
    # FURIA (Campea Americas Cup)
    {"team": "FURIA", "role": "top",     "game_name": "Guigo",    "tag_line": "BR1"},
    {"team": "FURIA", "role": "jungle",  "game_name": "Tatu",     "tag_line": "BR1"},
    {"team": "FURIA", "role": "mid",     "game_name": "Tutsz",    "tag_line": "BR1"},
    {"team": "FURIA", "role": "adc",     "game_name": "Ayu",      "tag_line": "BR1"},
    {"team": "FURIA", "role": "support", "game_name": "Jojo",     "tag_line": "BR1"},

    # LOUD
    {"team": "LOUD", "role": "top",     "game_name": "xyno",      "tag_line": "BR1"},
    {"team": "LOUD", "role": "jungle",  "game_name": "Youngjae",  "tag_line": "KR1"},
    {"team": "LOUD", "role": "mid",     "game_name": "Jean Mago", "tag_line": "BR1"},
    {"team": "LOUD", "role": "adc",     "game_name": "Bull",      "tag_line": "BR1"},
    {"team": "LOUD", "role": "support", "game_name": "Redbert",   "tag_line": "BR1"},

    # paiN Gaming
    {"team": "paiN Gaming", "role": "top",     "game_name": "Robo",     "tag_line": "BR1"},
    {"team": "paiN Gaming", "role": "jungle",  "game_name": "Cariok",   "tag_line": "BR1"},
    {"team": "paiN Gaming", "role": "mid",     "game_name": "tinowns",  "tag_line": "BR1"},
    {"team": "paiN Gaming", "role": "adc",     "game_name": "TitaN",    "tag_line": "BR1"},
    {"team": "paiN Gaming", "role": "support", "game_name": "Kuri",     "tag_line": "KR1"},

    # RED Canids Kalunga
    {"team": "RED Canids", "role": "top",     "game_name": "fNb",    "tag_line": "BR1"},
    {"team": "RED Canids", "role": "jungle",  "game_name": "DOOM",   "tag_line": "BR1"},
    {"team": "RED Canids", "role": "mid",     "game_name": "Kaze",   "tag_line": "BR1"},
    {"team": "RED Canids", "role": "adc",     "game_name": "Rabelo", "tag_line": "BR1"},
    {"team": "RED Canids", "role": "support", "game_name": "frosty", "tag_line": "BR1"},

    # Vivo Keyd Stars
    {"team": "Keyd Stars", "role": "top",     "game_name": "Boal",      "tag_line": "BR1"},
    {"team": "Keyd Stars", "role": "jungle",  "game_name": "Disamis",   "tag_line": "BR1"},
    {"team": "Keyd Stars", "role": "mid",     "game_name": "Mireu",     "tag_line": "KR1"},
    {"team": "Keyd Stars", "role": "adc",     "game_name": "Morttheus", "tag_line": "BR1"},
    {"team": "Keyd Stars", "role": "support", "game_name": "Kaiwing",   "tag_line": "KR1"},

    # Fluxo W7M
    {"team": "Fluxo W7M", "role": "top",     "game_name": "curty",    "tag_line": "BR1"},
    {"team": "Fluxo W7M", "role": "jungle",  "game_name": "Peach",    "tag_line": "KR1"},
    {"team": "Fluxo W7M", "role": "mid",     "game_name": "Hauz",     "tag_line": "BR1"},
    {"team": "Fluxo W7M", "role": "adc",     "game_name": "Bao",      "tag_line": "KR1"},
    {"team": "Fluxo W7M", "role": "support", "game_name": "ProDelta", "tag_line": "BR1"},

    # Leviatan (LLA)
    {"team": "Leviatan", "role": "top",     "game_name": "Devost",  "tag_line": "LAS"},
    {"team": "Leviatan", "role": "jungle",  "game_name": "Booki",   "tag_line": "LAS"},
    {"team": "Leviatan", "role": "mid",     "game_name": "Enga",    "tag_line": "LAS"},
    {"team": "Leviatan", "role": "adc",     "game_name": "Ceo",     "tag_line": "LAS"},
    {"team": "Leviatan", "role": "support", "game_name": "TopLop",  "tag_line": "LAS"},

    # LOS
    {"team": "LOS", "role": "top",     "game_name": "Zest",      "tag_line": "BR1"},
    {"team": "LOS", "role": "jungle",  "game_name": "Drakehero", "tag_line": "BR1"},
    {"team": "LOS", "role": "mid",     "game_name": "Feisty",    "tag_line": "BR1"},
    {"team": "LOS", "role": "adc",     "game_name": "Duduhh",    "tag_line": "BR1"},
    {"team": "LOS", "role": "support", "game_name": "Ackerman",  "tag_line": "BR1"},
]
