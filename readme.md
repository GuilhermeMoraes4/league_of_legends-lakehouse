# League of Legends Lakehouse — CBLOL Analytics

Pipeline end-to-end de Data Engineering para analytics de desempenho individual dos jogadores profissionais do CBLOL na Solo Queue. Extrai dados da Riot Games API (partidas ranked dos pros), processa via arquitetura medallion (Bronze/Silver/Gold) e serve dashboards com métricas como champion pool, KDA, CS/min, win rate por campeão e tendência de forma.

## Architecture

```
+--------------+     +------------------------+     +-------------------------------+
|  Riot Games  |     |   Airflow (Docker)     |     |  Databricks Community Ed.     |
|     API      |---->|                        |     |                               |
|              |     |  extract_accounts      |     |  UC Volumes (staging)         |
|  Account-V1  |     |  extract_match_ids     |     |  workspace.default.bronze/    |
|  Match-V5    |     |  extract_match_details |---->|         |                     |
|              |     |  load_to_databricks    |     |         v                     |
+--------------+     +------------------------+     |  Delta Tables (destino)       |
                       |  clock-sync (chrony) |     |  loldata.cblol_bronze.*       |
                       +------------------------+     |    accounts                   |
                                                    |    match_ids                  |
                                                    |    matches                    |
                                                    |    timelines                  |
                                                    +-------------------------------+
```

## Tech Stack

| Camada | Tecnologia |
|--------|-----------|
| Orquestração | Apache Airflow 3.x (Docker Compose, CeleryExecutor) |
| Processamento | PySpark + Delta Lake (Databricks Community Edition) |
| Storage | Delta Tables via Unity Catalog (bronze/silver/gold) + UC Volumes (staging) |
| Linguagem | Python 3.12 |
| Ambiente | WSL 2 (Ubuntu) no Windows |

## Setup local

### Pré-requisitos
- Docker + Docker Desktop
- Python 3.12
- Conta no [Databricks Community Edition](https://community.cloud.databricks.com/) (gratuito)

### 1. Configurar variáveis de ambiente

```bash
cp airflow/.env.example airflow/.env
# Editar airflow/.env:
#   AIRFLOW_UID=$(id -u)
#   RIOT_DEVELOPER_API=RGAPI-...  (renovar em developer.riotgames.com a cada 24h)
#   DATABRICKS_HOST=dbc-XXXXX.cloud.databricks.com  (sem https://)
#   DATABRICKS_TOKEN=dapi-...     (gerar em Settings > Developer > Access tokens, scope: all APIs)
```

### 2. Buildar a imagem e subir o Airflow

```bash
make build          # Builda a imagem com as dependências
make up             # Sobe os containers (inclui clock-sync para WSL2)
# Aguardar ~2 min. UI em http://localhost:8080 (airflow/airflow)
```

### 3. Instalar dependências de desenvolvimento

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
# Extração completa (com timelines)
python -m src.extract.main all --date 2026-04-07 --timeline

# Steps individuais
python -m src.extract.main accounts --date 2026-04-07
python -m src.extract.main match-ids --date 2026-04-07
python -m src.extract.main match-details --date 2026-04-07 --timeline

# Upload + carregamento em Delta tables
python -m src.upload.main --date 2026-04-07
```

## Estrutura de diretórios

```
lol-lakehouse/
+-- airflow/
|   +-- dags/                 # DAGs do Airflow (TaskFlow API)
|   +-- Dockerfile            # Imagem customizada (deps pre-instaladas)
|   +-- docker-compose.yaml   # Stack completa (Airflow + Redis + Postgres + clock-sync)
|   +-- .env                  # Secrets -- gitignored, copiar de .env.example
|   +-- .env.example          # Template sem secrets
+-- src/
|   +-- extract/              # Pipeline de extração da Riot API
|   +-- upload/               # Upload para Volumes + carregamento em Delta tables
+-- tests/unit/               # Testes unitários (pytest + responses)
+-- data/                     # Bronze layer local (gitignored)
+-- Makefile                  # Comandos utilitários
+-- pyproject.toml            # Packaging e config de ferramentas (ruff, mypy, pytest)
```

## DAG — Fluxo de execução (TaskFlow API)

```
extract_accounts >> extract_match_ids >> extract_match_details >> extract_timelines >> load_to_databricks
```

| Task | Descrição | Duração típica |
|------|-----------|----------------|
| `extract_accounts` | Resolve Riot IDs -> PUUIDs via Account-V1 (43 jogadores) | ~55s |
| `extract_match_ids` | Coleta match IDs ranked solo por jogador via Match-V5 | ~40s |
| `extract_match_details` | Baixa detalhes de cada partida via Match-V5 (~200 partidas) | ~5min |
| `extract_timelines` | Baixa timelines (eventos minuto a minuto) de cada partida | ~5min |
| `load_to_databricks` | Upload para Volumes + carregamento em Delta tables (SQL Warehouse) | ~3-5min |

O carregamento é em 2 fases: (1) upload para UC Volumes via Files API e (2) execução de SQL no Serverless SQL Warehouse para carregar em Delta tables. Se o warehouse não estiver configurado, os dados ficam nos Volumes.

## CBLOL 2026 — Times monitorados

8 times, 43 jogadores: FURIA, LOUD, paiN Gaming, RED Canids, Keyd Stars, Fluxo W7M, Leviatan, LOS.
Roster completo em `src/extract/config.py`.

## Objetivo de analytics

Analisar o desempenho individual dos jogadores profissionais do CBLOL na Solo Queue (Ranked Solo/Duo, queue 420). As partidas competitivas oficiais não são acessíveis via API pública (requerem Tournament API com aprovação da Riot), então o foco é na solo queue como proxy de skill individual.

Métricas planejadas para as camadas silver/gold:
- **Champion pool:** picks mais jogados por jogador, win rate por campeão
- **Performance individual:** KDA, CS/min, damage share, vision score, gold diff@15
- **Comparativo por role:** ranking entre tops, mids, adcs, etc.
- **Tendência/forma:** desempenho nos últimos N jogos
- **Meta do high elo BR:** campeões priorizados pelos pros

## Fases do projeto

| Fase | Status | Descrição |
|------|--------|-----------|
| 1 | Concluída | Infra inicial (migrada para Databricks CE gratuito) |
| 2 | Concluída | Airflow dockerizado, DAG skeleton |
| 3 | Concluída | Extração Riot API (accounts, matches, timelines) + carregamento em Delta tables via SQL Warehouse |
| 4 | Pendente | Processamento PySpark no Databricks CE (bronze -> silver -> gold) |
| 5 | Pendente | Dashboard de analytics (desempenho individual dos pros) |

## Decisões técnicas

- **Delta Tables no bronze** em vez de arquivos raw: dados salvos como Delta tables (`loldata.cblol_bronze.*`) com schema `raw_json STRING, extraction_date DATE`. UC Volumes usados como staging intermediário
- **SQL Statement Execution API**: execução remota de SQL no Serverless SQL Warehouse para carregar dados dos Volumes em Delta tables. Não requer cluster all-purpose
- **Clock-sync sidecar**: container `chrony` privilegiado no docker-compose que sincroniza o clock do kernel a cada 30s. Resolve o clock drift do WSL2 que quebra JWT interno do Airflow 3.x
- **`time.monotonic()`** no rate limiter: imune a ajustes de clock causados pelo NTP sync
- **Rate limiting conservador**: 1 request/1.3s (~46/min), bem abaixo do limite da dev key (100/2min)
- **Idempotência**: match details e uploads não são reprocessados se ja existem
- **Airflow 3.x compat**: helper `_get_ds()` na DAG para obter execution date (ds removido em runs manuais)

## Comandos úteis

```bash
make build       # Builda a imagem Docker (rodar 1x ou quando mudar requirements)
make up          # Sobe o Airflow
make down        # Para o Airflow
make logs        # Logs em tempo real
make test        # Roda os testes
make lint        # Lint com ruff
```

## Databricks Community Edition — Setup

1. Criar conta em [community.cloud.databricks.com](https://community.cloud.databricks.com/)
2. Gerar Personal Access Token em **Settings > Developer > Access tokens** (scope: all APIs)
3. Criar o volume de staging (1x): no SQL Editor, rodar `CREATE VOLUME workspace.default.bronze`
4. Criar o catalog e schema (1x): `CREATE CATALOG loldata; CREATE SCHEMA loldata.cblol_bronze`
5. Copiar o Warehouse ID: **Compute > SQL warehouses > Connection details > HTTP path** (último segmento)
6. Adicionar no `airflow/.env`:
   ```
   DATABRICKS_HOST=dbc-XXXXX.cloud.databricks.com
   DATABRICKS_TOKEN=dapi-seu-token-aqui
   DATABRICKS_WAREHOUSE_ID=xxxxxxxxxxxxxxxx
   ```
7. As tabelas Delta são criadas automaticamente no primeiro run em `loldata.cblol_bronze.*`
