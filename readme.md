# League of Legends Lakehouse — CBLOL Analytics

Pipeline end-to-end de Data Engineering para analytics de desempenho individual dos jogadores profissionais do CBLOL na Solo Queue. Extrai dados da Riot Games API (partidas ranked dos pros), processa via arquitetura medallion (Bronze/Silver/Gold) no Databricks e serve dashboards Power BI com metricas como champion pool, KDA, CS/min, DPM, GPM, draft analysis e tendencia de forma.

## Architecture

```
+--------------+     +------------------------+     +-------------------------------+
|  Riot Games  |     |   Airflow (Docker)     |     |  Databricks Community Ed.     |
|     API      |---->|                        |     |                               |
|              |     |  extract_accounts      |     |  UC Volumes (staging)         |
|  Account-V1  |     |  extract_match_ids     |     |  workspace.default.bronze/    |
|  Match-V5    |     |  extract_match_details |---->|         |                     |
|              |     |  extract_timelines     |     |         v                     |
+--------------+     |  load_to_databricks    |     |  loldata.cblol_bronze.*       |
                     +------------------------+     |    (raw JSON em Delta)        |
                                                    |         |                     |
                     +------------------------+     |         v                     |
                     |  upload_gold.py        |     |  loldata.cblol_silver.*       |
                     |  (SQL Warehouse API)   |---->|    (normalizado, tipado)      |
                     +------------------------+     |         |                     |
                                                    |         v                     |
                     +------------------------+     |  loldata.cblol_gold.*         |
                     |  Power BI              |<----|    (metricas agregadas)        |
                     |  Dashboards            |     |                               |
                     +------------------------+     +-------------------------------+
```

## Medalion Architecture — Delta Lake

### Bronze (raw)
JSON cru da Riot API em Delta tables. Schema: `raw_json STRING, extraction_date DATE`.

| Tabela | Conteudo |
|--------|----------|
| `accounts` | PUUIDs, gameName, tagLine dos jogadores monitorados |
| `match_ids` | IDs das partidas ranked de cada jogador |
| `matches` | Detalhes completos de cada partida (participants, teams, stats) |
| `timelines` | Eventos frame-a-frame de cada partida (posicao, gold, XP, kills) |

### Silver (normalizado)
JSONs explodidos em tabelas tipadas, 1 entidade por linha. MERGE idempotente.

| Tabela | Granularidade | Campos-chave |
|--------|---------------|--------------|
| `accounts` | 1 por jogador | puuid, game_name, team, role |
| `matches` | 1 por partida | match_id, game_duration, winning_team_id |
| `match_teams` | 1 por time/partida | objectives (baron, dragon, tower), bans |
| `match_participants` | 1 por jogador/partida | champion, kills, deaths, assists, kda, items |
| `timeline_frames` | 1 por jogador/minuto/partida | gold, xp, level, position |
| `timeline_events` | 1 por evento/partida | kills, item purchases, ward placements |

### Gold (analytics-ready)
Tabelas denormalizadas com metricas calculadas, prontas para dashboards Power BI.

| Tabela | Granularidade | Metricas |
|--------|---------------|----------|
| `gold_team_stats` | 1 por time/partida | Objetivos (baron, dragon, tower), resultado, duracao |
| `gold_player_performance` | 1 por jogador/partida | KDA, GPM, DPM, CSPM, damage/gold, vision, items |
| `gold_draft` | 1 por champion/type/team/partida | Picks e bans normalizados com champion names resolvidos |
| `gold_player_frames_indexed` | 1 por jogador/frame/partida | Timeline com indice sequencial, gold, xp, posicao |

## Tech Stack

| Camada | Tecnologia |
|--------|-----------|
| Orquestracao | Apache Airflow 3.x (Docker Compose, CeleryExecutor) |
| Processamento | PySpark + Delta Lake (Databricks Community Edition) |
| Storage | Delta Tables via Unity Catalog + UC Volumes (staging) |
| Analytics | Power BI (conectado em loldata.cblol_gold.*) |
| Linguagem | Python 3.12 |
| Ambiente | WSL 2 (Ubuntu) no Windows |

## Setup local

### Pre-requisitos
- Docker + Docker Desktop
- Python 3.12
- Conta no [Databricks Community Edition](https://community.cloud.databricks.com/) (gratuito)

### 1. Configurar variaveis de ambiente

```bash
cp airflow/.env.example airflow/.env
# Editar airflow/.env:
#   AIRFLOW_UID=$(id -u)
#   RIOT_DEVELOPER_API=RGAPI-...  (renovar em developer.riotgames.com a cada 24h)
#   DATABRICKS_HOST=dbc-XXXXX.cloud.databricks.com  (sem https://)
#   DATABRICKS_TOKEN=dapi-...     (gerar em Settings > Developer > Access tokens)
#   DATABRICKS_WAREHOUSE_ID=xxxxx (Compute > SQL warehouses > Connection details)
```

### 2. Buildar a imagem e subir o Airflow

```bash
make build          # Builda a imagem com as dependencias
make up             # Sobe os containers (inclui clock-sync para WSL2)
# Aguardar ~2 min. UI em http://localhost:8080 (airflow/airflow)
```

### 3. Instalar dependencias de desenvolvimento

```bash
make install-dev        # pip install -e ".[dev]"
make pre-commit-install
```

### 4. Rodar os testes

```bash
make test
```

### 5. Pipeline manual (sem Airflow)

```bash
# Extracao completa (com timelines)
python -m src.extract.main all --date 2026-04-07 --timeline

# Steps individuais
python -m src.extract.main accounts --date 2026-04-07
python -m src.extract.main match-ids --date 2026-04-07
python -m src.extract.main match-details --date 2026-04-07 --timeline

# Upload bronze (Volumes + Delta tables)
python -m src.upload.main --date 2026-04-07

# Carga gold (cria schema, popula tabelas via SQL Warehouse)
python -c "
from src.upload.upload_gold import upload_gold
upload_gold(host='SEU_HOST', token='SEU_TOKEN', warehouse_id='SEU_WAREHOUSE_ID')
"
```

## Estrutura de diretorios

```
lol-lakehouse/
+-- airflow/
|   +-- dags/                 # DAGs do Airflow (TaskFlow API)
|   +-- Dockerfile            # Imagem customizada (deps pre-instaladas)
|   +-- docker-compose.yaml   # Stack completa (Airflow + Redis + Postgres + clock-sync)
|   +-- .env.example          # Template sem secrets
+-- databricks/
|   +-- silver/               # Notebooks PySpark para camada silver
|   +-- gold/                 # Notebooks PySpark para camada gold
+-- specs/
|   +-- silver-layer.md       # Spec aprovada da camada silver
|   +-- gold-layer.md         # Spec aprovada da camada gold
+-- src/
|   +-- extract/              # Pipeline de extracao da Riot API
|   +-- upload/               # Upload bronze + carga gold via SQL Warehouse API
+-- tests/unit/               # Testes unitarios (pytest + responses)
+-- Makefile                  # Comandos utilitarios
+-- pyproject.toml            # Packaging e config de ferramentas
```

## DAG — Fluxo de execucao (TaskFlow API)

```
extract_accounts >> extract_match_ids >> extract_match_details >> extract_timelines >> load_to_databricks
```

| Task | Descricao | Duracao tipica |
|------|-----------|----------------|
| `extract_accounts` | Resolve Riot IDs -> PUUIDs via Account-V1 (43 jogadores) | ~55s |
| `extract_match_ids` | Coleta match IDs ranked solo por jogador via Match-V5 | ~40s |
| `extract_match_details` | Baixa detalhes de cada partida via Match-V5 (~200 partidas) | ~5min |
| `extract_timelines` | Baixa timelines (eventos minuto a minuto) de cada partida | ~5min |
| `load_to_databricks` | Upload para Volumes + carregamento em Delta tables | ~3-5min |

## CBLOL 2026 — Times monitorados

8 times, 43 jogadores: FURIA, LOUD, paiN Gaming, RED Canids, Keyd Stars, Fluxo W7M, Leviatan, LOS.
Roster completo em `src/extract/config.py`.

## Fases do projeto

| Fase | Status | Descricao |
|------|--------|-----------|
| 1 | Concluida | Infra inicial (migrada para Databricks CE gratuito) |
| 2 | Concluida | Airflow dockerizado, DAG skeleton |
| 3 | Concluida | Extracao Riot API + carregamento bronze via SQL Warehouse |
| 4 | Concluida | Processamento: silver (6 tabelas normalizadas) + gold (4 tabelas analytics) |
| 5 | Em andamento | Dashboards Power BI (desempenho individual dos pros) |

## Decisoes tecnicas

- **Arquitetura medallion (Bronze/Silver/Gold):** separacao clara entre dados raw, normalizados e analytics-ready. Silver usa MERGE INTO idempotente; gold calcula metricas derivadas (GPM, DPM, CSPM, damage/gold)
- **SQL Statement Execution API:** execucao remota de SQL no SQL Warehouse para carregar dados. Usado tanto no bronze (upload) quanto no gold (transformacoes). Nao requer cluster all-purpose
- **Deduplicacao por ROW_NUMBER:** antes de cada MERGE INTO, a source query usa `ROW_NUMBER() OVER(PARTITION BY pk ORDER BY extraction_date DESC)` para garantir 1 linha por PK
- **Filtro de remakes:** partidas com duracao < 300s sao excluidas de todas as tabelas gold
- **Clock-sync sidecar:** container chrony privilegiado no docker-compose que sincroniza o clock a cada 30s. Resolve o clock drift do WSL2 que quebra JWT interno do Airflow 3.x
- **Rate limiting conservador:** 1 request/1.3s (~46/min), bem abaixo do limite da dev key (100/2min)
- **Idempotencia end-to-end:** match details, uploads e MERGEs nao duplicam dados em re-runs

## Comandos uteis

```bash
make build       # Builda a imagem Docker (1x ou quando mudar requirements)
make up          # Sobe o Airflow
make down        # Para o Airflow
make logs        # Logs em tempo real
make test        # Roda os testes
make lint        # Lint com ruff
```

## Databricks Community Edition — Setup

1. Criar conta em [community.cloud.databricks.com](https://community.cloud.databricks.com/)
2. Gerar Personal Access Token em **Settings > Developer > Access tokens**
3. Criar o volume de staging (1x): `CREATE VOLUME workspace.default.bronze`
4. Criar catalog e schemas (1x):
   ```sql
   CREATE CATALOG loldata;
   CREATE SCHEMA loldata.cblol_bronze;
   CREATE SCHEMA loldata.cblol_silver;
   CREATE SCHEMA loldata.cblol_gold;
   ```
5. Copiar o Warehouse ID: **Compute > SQL warehouses > Connection details**
6. Adicionar no `airflow/.env` (ver `.env.example`)
7. As tabelas Delta sao criadas automaticamente pelo pipeline
