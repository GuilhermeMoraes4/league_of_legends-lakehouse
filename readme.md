# League of Legends Lakehouse — CBLOL Analytics

Pipeline end-to-end de Data Engineering para analytics do CBLOL. Extrai dados da Riot Games API, processa via arquitetura medallion (Bronze/Silver/Gold) e serve dashboards de performance de jogadores profissionais.

## Architecture

```
+--------------+     +------------------------+     +-------------------------------+
|  Riot Games  |     |   Airflow (Docker)     |     |  Databricks Community Ed.     |
|     API      |---->|                        |     |                               |
|              |     |  extract_accounts      |     |  Unity Catalog Volumes        |
|  Account-V1  |     |  extract_match_ids     |     |  workspace.default.bronze/    |
|  Match-V5    |     |  extract_match_details |---->|    accounts/                  |
|              |     |  upload_to_dbfs        |     |    match_ids/                 |
+--------------+     +------------------------+     |    matches/                   |
                       |  clock-sync (chrony) |     |                               |
                       +------------------------+     |  Notebooks PySpark           |
                                                    |  bronze -> silver -> gold     |
                                                    +-------------------------------+
```

## Tech Stack

| Camada | Tecnologia |
|--------|-----------|
| Orquestracao | Apache Airflow 3.x (Docker Compose, CeleryExecutor) |
| Processamento | PySpark + Delta Lake (Databricks Community Edition) |
| Storage | Unity Catalog Volumes (bronze JSON) + Delta Tables (silver/gold) |
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
#   DATABRICKS_TOKEN=dapi-...     (gerar em Settings > Developer > Access tokens, scope: all APIs)
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
# Extracao completa
python -m src.extract.main --date 2026-04-06

# Steps individuais
python -m src.extract.main accounts --date 2026-04-06
python -m src.extract.main match-ids --date 2026-04-06
python -m src.extract.main match-details --date 2026-04-06
```

## Estrutura de diretorios

```
lol-lakehouse/
+-- airflow/
|   +-- dags/                 # DAGs do Airflow (TaskFlow API)
|   +-- Dockerfile            # Imagem customizada (deps pre-instaladas)
|   +-- docker-compose.yaml   # Stack completa (Airflow + Redis + Postgres + clock-sync)
|   +-- .env                  # Secrets -- gitignored, copiar de .env.example
|   +-- .env.example          # Template sem secrets
+-- src/
|   +-- extract/              # Pipeline de extracao da Riot API
|   +-- upload/               # Upload bronze local -> Databricks UC Volumes
+-- tests/unit/               # Testes unitarios (pytest + responses)
+-- data/                     # Bronze layer local (gitignored)
+-- Makefile                  # Comandos utilitarios
+-- pyproject.toml            # Packaging e config de ferramentas (ruff, mypy, pytest)
```

## DAG — Fluxo de execucao (TaskFlow API)

```
extract_accounts >> extract_match_ids >> extract_match_details >> upload_to_dbfs
```

| Task | Descricao | Duracao tipica |
|------|-----------|----------------|
| `extract_accounts` | Resolve Riot IDs -> PUUIDs via Account-V1 (40 jogadores) | ~55s |
| `extract_match_ids` | Coleta match IDs por jogador via Match-V5 | ~40s |
| `extract_match_details` | Baixa detalhes de cada partida via Match-V5 (~200 partidas) | ~5min |
| `upload_to_dbfs` | Envia JSONs para Databricks UC Volumes (Files API) | ~3-4min |

O upload eh graceful: se `DATABRICKS_HOST` ou `DATABRICKS_TOKEN` nao estiverem configurados, a task passa sem erro (os dados ficam apenas locais).

## CBLOL 2026 — Times monitorados

8 times, 40 jogadores: FURIA, LOUD, paiN Gaming, RED Canids, Keyd Stars, Fluxo W7M, Leviatan, LOS.
Roster completo em `src/extract/config.py`.

## Fases do projeto

| Fase | Status | Descricao |
|------|--------|-----------|
| 1 | Concluida | Infra inicial (migrada para Databricks CE gratuito) |
| 2 | Concluida | Airflow dockerizado, DAG skeleton |
| 3 | Concluida | Extracao Riot API + upload Databricks + integracao na DAG |
| 4 | Pendente | Processamento PySpark no Databricks CE (bronze -> silver -> gold) |
| 5 | Pendente | Dashboard de analytics |

## Decisoes tecnicas

- **Unity Catalog Volumes** em vez de DBFS: o Databricks desabilitou o DBFS publico por seguranca. Upload via Files API (`PUT /api/2.0/fs/files/Volumes/...`)
- **Clock-sync sidecar**: container `chrony` privilegiado no docker-compose que sincroniza o clock do kernel a cada 30s. Resolve o clock drift do WSL2 que quebra JWT interno do Airflow 3.x
- **`time.monotonic()`** no rate limiter: imune a ajustes de clock causados pelo NTP sync
- **Rate limiting conservador**: 1 request/1.3s (~46/min), bem abaixo do limite da dev key (100/2min)
- **Idempotencia**: match details e uploads nao sao reprocessados se ja existem
- **Airflow 3.x compat**: helper `_get_ds()` na DAG para obter execution date (ds removido em runs manuais)

## Comandos uteis

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
3. Adicionar no `airflow/.env`:
   ```
   DATABRICKS_HOST=dbc-XXXXX.cloud.databricks.com
   DATABRICKS_TOKEN=dapi-seu-token-aqui
   ```
4. Criar o volume no Databricks (1x): no SQL Editor, rodar `CREATE VOLUME workspace.default.bronze`
5. Os dados bronze serao enviados automaticamente para `workspace.default.bronze/` via Files API
6. Para processar silver/gold: ligar o cluster, rodar os notebooks
