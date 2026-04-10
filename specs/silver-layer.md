# Spec: Camada Silver — loldata.cblol_silver

**Status:** approved
**Data:** 2026-04-10
**Aprovado por:** Guilherme Moraes

---

## Objetivo

Transformar as 4 tabelas bronze (`loldata.cblol_bronze`) em 6 tabelas silver normalizadas (`loldata.cblol_silver`), com JSONs explodidos, campos tipados e prontas para agregacao na gold.

## Estrutura Bronze (input)

Todas as tabelas bronze tem 2 colunas:
- `raw_json` (STRING) — JSON cru da Riot API
- `extraction_date` (DATE) — data de ingestao

| Tabela | Origem API | Conteudo |
|---|---|---|
| `accounts` | Account-V1 / Summoner-V4 | puuid, gameName, tagLine, summonerLevel, profileIconId |
| `match_ids` | Match-V5 (list) | match_id, puuid de referencia |
| `matches` | Match-V5 (detail) | JSON: metadata, info.participants[] (10 jogadores), info.teams[] (2 times) |
| `timelines` | Match-V5 (timeline) | JSON de frames: participantFrames{} por minuto, events[] |

## Tabelas Silver (output) — loldata.cblol_silver

| # | Tabela | Origem | Granularidade |
|---|---|---|---|
| 1 | `accounts` | bronze.accounts | 1 linha por jogador (puuid) |
| 2 | `matches` | bronze.matches (campos raiz) | 1 linha por partida (match_id) |
| 3 | `match_teams` | bronze.matches → info.teams[] | 1 linha por time por partida (match_id + team_id) |
| 4 | `match_participants` | bronze.matches → info.participants[] | 1 linha por jogador por partida (match_id + puuid) |
| 5 | `timeline_frames` | bronze.timelines → frames[].participantFrames | 1 linha por jogador por minuto por partida |
| 6 | `timeline_events` | bronze.timelines → frames[].events[] | 1 linha por evento por partida |

## Campos por tabela

### 1. accounts
- `puuid` (STRING, PK)
- `game_name` (STRING)
- `tag_line` (STRING)
- `summoner_level` (INT)
- `profile_icon_id` (INT)
- `extraction_date` (DATE)

### 2. matches
- `match_id` (STRING, PK)
- `game_creation` (TIMESTAMP)
- `game_duration_seconds` (INT)
- `game_end_timestamp` (TIMESTAMP)
- `game_version` (STRING)
- `game_mode` (STRING)
- `game_type` (STRING)
- `map_id` (INT)
- `queue_id` (INT)
- `platform_id` (STRING)
- `winning_team_id` (INT)
- `extraction_date` (DATE)

### 3. match_teams
- `match_id` (STRING, PK)
- `team_id` (INT, PK)
- `win` (BOOLEAN)
- `baron_kills` (INT)
- `dragon_kills` (INT)
- `tower_kills` (INT)
- `inhibitor_kills` (INT)
- `rift_herald_kills` (INT)
- `bans` (ARRAY<STRING>)
- `extraction_date` (DATE)

### 4. match_participants
- `match_id` (STRING, PK)
- `puuid` (STRING, PK)
- `team_id` (INT)
- `champion_name` (STRING)
- `champion_id` (INT)
- `individual_position` (STRING)
- `role` (STRING)
- `kills` (INT)
- `deaths` (INT)
- `assists` (INT)
- `kda` (DOUBLE) — calculado: (kills + assists) / greatest(deaths, 1)
- `gold_earned` (INT)
- `total_minions_killed` (INT)
- `neutral_minions_killed` (INT)
- `vision_score` (INT)
- `wards_placed` (INT)
- `wards_killed` (INT)
- `total_damage_dealt_to_champions` (INT)
- `total_damage_taken` (INT)
- `total_heal` (INT)
- `item0` (INT)
- `item1` (INT)
- `item2` (INT)
- `item3` (INT)
- `item4` (INT)
- `item5` (INT)
- `item6` (INT)
- `summoner1_id` (INT)
- `summoner2_id` (INT)
- `win` (BOOLEAN)
- `extraction_date` (DATE)

### 5. timeline_frames
- `match_id` (STRING, PK)
- `participant_id` (INT, PK)
- `timestamp_ms` (LONG, PK)
- `minute` (INT) — calculado: floor(timestamp_ms / 60000)
- `current_gold` (INT)
- `total_gold` (INT)
- `xp` (INT)
- `level` (INT)
- `minions_killed` (INT)
- `jungle_minions_killed` (INT)
- `position_x` (INT)
- `position_y` (INT)
- `extraction_date` (DATE)

### 6. timeline_events
- `match_id` (STRING, PK)
- `timestamp_ms` (LONG, PK)
- `event_index` (INT, PK) — indice do evento dentro do frame para garantir unicidade
- `minute` (INT) — calculado: floor(timestamp_ms / 60000)
- `type` (STRING) — CHAMPION_KILL, ITEM_PURCHASED, WARD_PLACED, BUILDING_KILL, etc.
- `killer_id` (INT)
- `victim_id` (INT)
- `assisting_participant_ids` (ARRAY<INT>)
- `position_x` (INT)
- `position_y` (INT)
- `item_id` (INT)
- `ward_type` (STRING)
- `building_type` (STRING)
- `extraction_date` (DATE)

## Design

Cada notebook segue o padrao:

```python
# 1. Leitura da bronze
df = spark.read.table("loldata.cblol_bronze.<tabela>")

# 2. Parse do JSON cru com schema explicito
schema = StructType([...])
df_parsed = df.withColumn("data", from_json(col("raw_json"), schema))

# 3. Transformacao (explode, select, cast, colunas calculadas)
df_silver = df_parsed.select(...)

# 4. Escrita com MERGE (idempotente)
# CREATE TABLE IF NOT EXISTS + MERGE INTO usando PKs
```

Notebooks sao independentes. Ordem logica:
1. accounts (sem dependencia)
2. matches (base para joins)
3. match_teams + match_participants (paralelo)
4. timeline_frames + timeline_events (paralelo)

## Testes

### Assertions inline (em cada notebook)
- `count > 0` — tabela nao vazia
- `null check PKs` — chaves primarias nunca nulas
- `no duplicates` — PKs distintas == contagem total
- `timestamp range` — entre 2020 e hoje (matches, timeline_*)
- `kda >= 0` — nunca negativo (match_participants)
- `team_id in (100, 200)` — valores validos (match_teams, match_participants)
- `minute >= 0` — nunca negativo (timeline_frames, timeline_events)

### Notebook de validacao referencial (silver_validation.py)
1. Todo match_id em match_teams existe em matches
2. Todo match_id em match_participants existe em matches
3. Todo puuid em match_participants existe em accounts
4. Todo match_id em timeline_frames existe em matches
5. Todo match_id em timeline_events existe em matches
6. Contagem de participantes por partida == 10
7. Contagem de times por partida == 2

## Arquivos a criar

```
databricks/
  silver/
    silver_accounts.py
    silver_matches.py
    silver_match_teams.py
    silver_match_participants.py
    silver_timeline_frames.py
    silver_timeline_events.py
    silver_validation.py
```
