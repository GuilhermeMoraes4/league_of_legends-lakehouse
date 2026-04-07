# League of Legends Lakehouse - Project Context

## O que eh este projeto
Pipeline end-to-end de Data Engineering para analytics de desempenho individual dos jogadores profissionais do CBLOL na Solo Queue. Extrai dados da Riot Games API (accounts, matches, timelines), carrega em Delta tables no Databricks via SQL Warehouse, e serve dashboards com metricas como champion pool, KDA, CS/min, win rate por campeao e tendencia de forma.

## Stack
- **Processamento:** PySpark + Delta Lake no Databricks Community Edition (gratuito)
- **Orquestracao:** Apache Airflow 3.x via Docker Compose (CeleryExecutor, PostgreSQL, Redis)
- **Storage:** Delta Tables via Unity Catalog (loldata.cblol_bronze.*) + UC Volumes (staging)
- **Linguagem:** Python 3.12
- **Ambiente:** WSL 2 (Ubuntu) no Windows

## Estrutura de diretorios
```
lol-lakehouse/
├── airflow/
│   ├── dags/                    # DAG files (TaskFlow API)
│   ├── config/                  # airflow.cfg
│   ├── logs/                    # gerado em runtime (gitignored)
│   ├── plugins/
│   ├── Dockerfile               # Imagem customizada (deps pre-instaladas)
│   ├── .env                     # secrets (gitignored)
│   ├── .env.example             # template do .env sem secrets
│   └── docker-compose.yaml      # stack Airflow completa (CeleryExecutor + clock-sync)
├── src/
│   ├── __init__.py
│   ├── extract/
│   │   ├── config.py            # Players CBLOL 2026, endpoints, rate limits
│   │   ├── riot_api_client.py   # HTTP client com rate limiting e retries
│   │   ├── extract_accounts.py  # Account-V1: Riot ID -> PUUID
│   │   ├── extract_matches.py   # Match-V5: match IDs, detalhes e timelines
│   │   └── main.py              # Orquestrador extracao (chamado pela DAG)
│   └── upload/
│       ├── config.py            # Databricks host, token, warehouse ID, catalog/schema
│       ├── dbfs_client.py       # HTTP client para Files API 2.0 (UC Volumes staging)
│       ├── databricks_sql.py    # SQL Statement Execution API (SQL Warehouse)
│       ├── upload_bronze.py     # Upload Volumes + carregamento Delta tables
│       └── main.py              # Orquestrador upload (chamado pela DAG)
├── tests/
│   └── unit/
│       ├── test_riot_api_client.py
│       ├── test_extract_accounts.py
│       ├── test_extract_matches.py
│       ├── test_dbfs_client.py
│       └── test_upload_bronze.py
├── data/                        # Bronze layer local (gitignored)
├── .editorconfig
├── .gitignore
├── .pre-commit-config.yaml
├── Makefile
├── pyproject.toml
├── requirements.txt             # runtime
└── CLAUDE.md
```

## Riot Games API - Endpoints usados
- **Account-V1:** `GET https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}` -> PUUID
- **Match-V5 (IDs):** `GET https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids` -> lista de match IDs
- **Match-V5 (Details):** `GET https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}` -> detalhes completos
- **Match-V5 (Timeline):** `GET https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}/timeline` -> eventos minuto a minuto
- **Summoner-V4:** `GET https://br1.api.riotgames.com/lol/summoner/v4/summoners/by-puuid/{puuid}` -> perfil

### Rate limits (dev key)
- 20 requests/segundo, 100 requests/2 minutos
- Key expira a cada 24h (regenerar em developer.riotgames.com)
- Autenticacao: header `X-Riot-Token: RGAPI-xxxxx`
- Rate limit retornado via headers `X-App-Rate-Limit`, `X-Method-Rate-Limit`
- HTTP 429 = rate limited, respeitar header `Retry-After`

### Regioes
- Platform (summoner, league): `br1.api.riotgames.com`
- Regional (account, match): `americas.api.riotgames.com`

## CBLOL 2026 - Times e jogadores monitorados
8 times, 43 jogadores: FURIA, LOUD, paiN Gaming, RED Canids, Keyd Stars, Fluxo W7M, Leviatan, LOS.
Roster completo em `src/extract/config.py`. TagLine padrao BR: `BR1`. Coreanos: `KR1`. LLA: `LAS`.

## Fases do projeto
1. **Fase 1 (concluida):** Infra inicial (descontinuada — migrado de Azure pago para Databricks CE gratuito)
2. **Fase 2 (concluida):** Airflow dockerizado, DAG skeleton
3. **Fase 3 (concluida):** Extracao Riot API (accounts, matches, timelines) + carregamento em Delta tables via SQL Warehouse
4. **Fase 4 (pendente):** Processamento PySpark no Databricks CE (bronze -> silver -> gold)
5. **Fase 5 (pendente):** Dashboard de analytics

## Databricks
- Autenticacao: Personal Access Token (PAT) gerado em Settings > Developer > Access tokens
- SQL Warehouse: Serverless Starter Warehouse (warehouse_id no .env)
- Upload staging: Files API 2.0 para UC Volumes (`PUT /api/2.0/fs/files/Volumes/...`)
- Carregamento: SQL Statement Execution API (`POST /api/2.0/sql/statements/`) para Delta tables
- DBFS publico desabilitado pelo Databricks — usar Unity Catalog Volumes como staging

### Delta Tables (bronze layer)
```
loldata.cblol_bronze.accounts      (raw_json STRING, extraction_date DATE)
loldata.cblol_bronze.match_ids     (raw_json STRING, extraction_date DATE)
loldata.cblol_bronze.matches       (raw_json STRING, extraction_date DATE)
loldata.cblol_bronze.timelines     (raw_json STRING, extraction_date DATE)
```

### UC Volumes (staging)
```
workspace.default.bronze/
├── accounts/dt=YYYY-MM-DD/accounts.json
├── match_ids/dt=YYYY-MM-DD/match_ids.json
├── match_ids/dt=YYYY-MM-DD/player_match_map.json
├── matches/dt=YYYY-MM-DD/{match_id}.json
└── timelines/dt=YYYY-MM-DD/{match_id}_timeline.json
```

### Fluxo de dados
```
Riot Games API  ->  Airflow (Docker)  ->  data/bronze/ (local)  ->  UC Volumes (staging)
                    (automatico)          (automatico)               (Files API)
                                                                         |
                                                                         v
                                                                    Delta Tables
                                                                    loldata.cblol_bronze.*
                                                                    (SQL Statement Execution API)
```

## Convencoes de codigo
- Python 3.12, sem emojis no codigo
- Docstrings em portugues (informal, direto)
- Logging estruturado (nao usar print)
- Nenhum comentario deve ser removido sem motivo explicito
- Imports absolutos (`from src.extract.config import ...`)
- JSON como formato de output no bronze layer, particionado por data (`dt=YYYY-MM-DD`)
- Nao usar numeros de linha em referencias de codigo

## Airflow
- Docker Compose com CeleryExecutor — `make build` (1x) + `make up` da raiz
- Dockerfile customizado em `airflow/Dockerfile` — deps pre-instaladas na imagem (sem `_PIP_ADDITIONAL_REQUIREMENTS`)
- `PYTHONPATH=/opt/airflow` no docker-compose — imports `from src.extract...` funcionam sem hack
- DAGs em `airflow/dags/` usando TaskFlow API (`@dag`, `@task` decorators)
- `src/` montado em `/opt/airflow/src` — hot-reload durante desenvolvimento
- `data/` montado em `/opt/airflow/data` — bronze layer persistido fora do container
- Variaveis de ambiente injetadas via `airflow/.env`: `RIOT_DEVELOPER_API`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`
- DAG principal: `riot_api_ingestion` (schedule @daily, start_date 2026-02-25)
- Flow: extract_accounts >> extract_match_ids >> extract_match_details >> extract_timelines >> load_to_databricks
- `mem_limit` em todos os containers para proteger RAM do WSL2
- Container `clock-sync` (chrony privilegiado) sincroniza o clock do kernel a cada 30s (fix WSL2 clock drift)
- `DATABRICKS_HOST` no .env deve ser sem protocolo (ex: `dbc-XXXXX.cloud.databricks.com`, sem `https://`)

### Airflow 3.x — Particularidades
- `ds` (execution date string) nao existe no context de runs manuais sem logical_date
- DAG usa helper `_get_ds()` que tenta `ds`, depois `logical_date.strftime()`, depois `datetime.now()`
- JWT interno entre worker e API server eh sensivel a clock drift (>5s causa falha)
- `TASK_INSTANCE_HEARTBEAT_TIMEOUT` aumentado para 900s no docker-compose
- `SCHEDULER_ZOMBIE_TASK_TIMEOUT` aumentado para 1800s

## Decisoes de design
- Rate limiting conservador: 1 request a cada 1.3s (~46/min), bem abaixo do limite
- Rate limiter usa `time.monotonic()` (imune a ajustes de clock por NTP/chrony no Docker/WSL2)
- Idempotencia: match details e timelines nao sao re-extraidos se o JSON ja existe
- Idempotencia no upload: arquivos ja existentes nos Volumes sao pulados (HEAD check)
- Exponential backoff em 429 e 5xx (tanto Riot API quanto Databricks)
- Deduplicacao de match IDs (jogadores do mesmo time compartilham partidas)
- Bronze layer = JSON raw em Delta tables (raw_json STRING + extraction_date DATE)
- Upload em 2 fases: Files API para Volumes (staging) + SQL Statement Execution API para Delta tables
- Graceful degradation: se DATABRICKS_HOST/TOKEN nao configurados, upload ignorado. Se WAREHOUSE_ID nao configurado, dados ficam nos Volumes
- Timelines extraidas como task independente na DAG (idempotencia separada dos match details)
- Client unico no `run_pipeline` (modo `all`): 1 health check em vez de 3
- Dockerfile customizado: deps pre-instaladas na imagem, sem instalacao a cada restart
- TaskFlow API na DAG: imports Python diretos em vez de BashOperator + subprocess
- Clock-sync sidecar no docker-compose: chrony privilegiado corrige drift do WSL2 Docker Desktop
