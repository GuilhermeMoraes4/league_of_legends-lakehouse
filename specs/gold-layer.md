# Spec: Camada Gold — loldata.cblol_gold

**Status:** approved
**Data:** 2026-04-16
**Aprovado por:** Guilherme Moraes

---

## Objetivo

Transformar as 6 tabelas silver (`loldata.cblol_silver`) em 4 tabelas gold denormalizadas/agregadas (`loldata.cblol_gold`), prontas para consumo direto pelos dashboards Power BI. As transformacoes replicam os tratamentos que seriam feitos no Power Query, movendo a logica para o Databricks.

## Referencia

Tratamentos baseados nos videos do canal Lanterninho:
- Video 1 (hwRTbYG0fTU): Tipagem, importacao, image URLs
- Video 2 (Xm7JaZGw6M8): Player frames indexados, picks/bans normalizados, teams separados
- Video 3 (ZCrn-xN7htE): Relacionamentos, KDA, DPM, GPM, CSPM, visual de performance

## Pre-requisito: Amend silver_match_participants

A `silver_timeline_frames` usa `participant_id` (1-10) como PK, mas `silver_match_participants` nao tem esse campo. Sem ele, nao ha como fazer JOIN direto para resolver `participant_id -> puuid/champion_name` na `gold_player_frames_indexed`.

**Acao:** Adicionar `participant_id INT` a `silver_match_participants`.
- Fonte: `info.participants[].participantId` no JSON do bronze
- Requer: ALTER TABLE + re-populacao via notebook silver atualizado
- Deve ser executado ANTES dos notebooks gold

### SQL para o amend

```sql
-- 1. Adicionar coluna
ALTER TABLE loldata.cblol_silver.match_participants ADD COLUMN participant_id INT;

-- 2. Popular a partir do bronze (re-parse do JSON)
-- O notebook silver_match_participants.py atualizado deve:
--   a) Extrair participantId do JSON junto com os demais campos
--   b) MERGE INTO com a nova coluna
```

## Estrutura Silver (input)

| Tabela | Granularidade | PK |
|---|---|---|
| `accounts` | 1 por jogador | puuid |
| `matches` | 1 por partida | match_id |
| `match_teams` | 1 por time/partida | match_id + team_id |
| `match_participants` | 1 por jogador/partida | match_id + puuid |
| `timeline_frames` | 1 por jogador/minuto/partida | match_id + participant_id + timestamp_ms |
| `timeline_events` | 1 por evento/partida | match_id + timestamp_ms + event_index |

### Campos relevantes por tabela silver

**accounts:** puuid, game_name, tag_line, team, summoner_level, profile_icon_id, extraction_date

**matches:** match_id, game_creation (TIMESTAMP), game_duration_seconds (INT), game_end_timestamp, game_version, game_mode, game_type, map_id, queue_id, platform_id, winning_team_id, extraction_date

**match_teams:** match_id, team_id, win, baron_kills, dragon_kills, tower_kills, inhibitor_kills, rift_herald_kills, bans (ARRAY<STRING> de champion_ids), extraction_date

**match_participants:** match_id, puuid, team_id, champion_name, champion_id, individual_position, role, kills, deaths, assists, kda (DOUBLE), gold_earned, total_minions_killed, neutral_minions_killed, vision_score, wards_placed, wards_killed, total_damage_dealt_to_champions, total_damage_taken, total_heal, item0-6, summoner1_id, summoner2_id, win, extraction_date
- **Apos amend:** + participant_id (INT)

**timeline_frames:** match_id, participant_id, timestamp_ms, minute, current_gold, total_gold, xp, level, minions_killed, jungle_minions_killed, position_x, position_y, extraction_date

## Tabelas Gold (output) — loldata.cblol_gold

| # | Tabela | Origem | Granularidade |
|---|---|---|---|
| 1 | `gold_team_stats` | match_teams + matches | 1 linha por time por partida |
| 2 | `gold_player_performance` | match_participants + matches + accounts | 1 linha por jogador por partida |
| 3 | `gold_draft` | match_teams (bans) + match_participants (picks) | 1 linha por champion por type por team por partida |
| 4 | `gold_player_frames_indexed` | timeline_frames + match_participants | 1 linha por jogador por frame por partida |

## Filtro global

Todas as tabelas gold EXCLUEM partidas com `game_duration_seconds < 300` (remakes).

## Campos por tabela

### 1. gold_team_stats

| Coluna | Tipo | Origem |
|--------|------|--------|
| match_id | STRING | PK |
| team_id | INT | PK |
| win | BOOLEAN | match_teams |
| baron_kills | INT | match_teams |
| dragon_kills | INT | match_teams |
| tower_kills | INT | match_teams |
| inhibitor_kills | INT | match_teams |
| rift_herald_kills | INT | match_teams |
| game_duration_seconds | INT | matches (JOIN) |
| game_creation | TIMESTAMP | matches (JOIN) |
| winning_team_id | INT | matches (JOIN) |

**PK:** (match_id, team_id)
**JOIN:** `silver.match_teams t JOIN silver.matches m ON t.match_id = m.match_id`
**Filtro:** `m.game_duration_seconds >= 300`

**Validacoes:**
- Exatamente 2 linhas por match_id
- team_id IN (100, 200)
- win coerente com winning_team_id (WHERE team_id = winning_team_id THEN win = true)

### 2. gold_player_performance

| Coluna | Tipo | Origem |
|--------|------|--------|
| match_id | STRING | PK |
| puuid | STRING | PK |
| game_name | STRING | accounts (JOIN) |
| tag_line | STRING | accounts (JOIN) |
| team | STRING | accounts (JOIN) |
| team_id | INT | match_participants |
| champion_name | STRING | match_participants |
| champion_id | INT | match_participants |
| role | STRING | match_participants |
| individual_position | STRING | match_participants |
| kills | INT | match_participants |
| deaths | INT | match_participants |
| assists | INT | match_participants |
| kda | DOUBLE | match_participants (herdado, nao recalcular) |
| gold_earned | INT | match_participants |
| total_minions_killed | INT | match_participants (lane CS) |
| neutral_minions_killed | INT | match_participants (jungle CS) |
| total_cs | INT | **calculado:** total_minions_killed + neutral_minions_killed |
| vision_score | INT | match_participants |
| wards_placed | INT | match_participants |
| wards_killed | INT | match_participants |
| total_damage_dealt_to_champions | INT | match_participants |
| total_damage_taken | INT | match_participants |
| total_heal | INT | match_participants |
| gold_per_minute | DOUBLE | **calculado:** gold_earned / (game_duration_seconds / 60.0) |
| damage_per_minute | DOUBLE | **calculado:** total_damage_dealt_to_champions / (game_duration_seconds / 60.0) |
| cs_per_minute | DOUBLE | **calculado:** total_cs / (game_duration_seconds / 60.0) |
| damage_per_gold | DOUBLE | **calculado:** total_damage_dealt_to_champions / GREATEST(gold_earned, 1) |
| item0 | INT | match_participants |
| item1 | INT | match_participants |
| item2 | INT | match_participants |
| item3 | INT | match_participants |
| item4 | INT | match_participants |
| item5 | INT | match_participants |
| item6 | INT | match_participants |
| summoner1_id | INT | match_participants |
| summoner2_id | INT | match_participants |
| win | BOOLEAN | match_participants |
| game_duration_seconds | INT | matches (JOIN) |

**PK:** (match_id, puuid)
**JOIN:** `silver.match_participants p JOIN silver.matches m ON p.match_id = m.match_id JOIN silver.accounts a ON p.puuid = a.puuid`
**Filtro:** `m.game_duration_seconds >= 300`
**KDA:** herdado da silver (nao recalcular)

**Nota sobre CS:** `total_minions_killed` = lane minions (1 CS cada). `neutral_minions_killed` = jungle monsters (contagem individual por monstro). `total_cs` = soma simples dos dois, igual ao scoreboard in-game.

**Validacoes:**
- Exatamente 10 linhas por match_id
- Metricas per-minute >= 0
- gold_earned > 0
- PKs nao-nulas e unicas

### 3. gold_draft

| Coluna | Tipo | Origem |
|--------|------|--------|
| match_id | STRING | PK |
| team_id | INT | PK |
| champion_id | INT | PK |
| type | STRING | PK — "pick" ou "ban" |
| champion_name | STRING | resolvido via CTE |
| draft_order | INT | posicao no array (bans) ou NULL (picks) |

**PK:** (match_id, team_id, champion_id, type)
**Origem bans:**
```sql
SELECT match_id, team_id, CAST(ban_id AS INT) AS champion_id, 'ban' AS type, pos + 1 AS draft_order
FROM silver.match_teams
LATERAL VIEW posexplode(bans) AS pos, ban_id
WHERE CAST(ban_id AS INT) != -1  -- filtrar bans vazios
```
**Origem picks:**
```sql
SELECT match_id, team_id, champion_id, 'pick' AS type, NULL AS draft_order
FROM silver.match_participants
```
**Resolucao champion_name:**
```sql
-- CTE com mapeamento champion_id -> champion_name
WITH champion_map AS (
    SELECT DISTINCT champion_id, champion_name
    FROM silver.match_participants
)
```
**UNION ALL** entre bans e picks, JOIN com champion_map
**Filtro:** matches com game_duration_seconds < 300 excluidos

**Validacoes:**
- Cada match tem entre 10-20 linhas (10 picks + ate 10 bans)
- type IN ('pick', 'ban')
- Sem champion_id nulo ou -1
- Bans nao resolvidos (champion_id sem match no champion_map) devem ser sinalizados com champion_name = 'UNKNOWN'

### 4. gold_player_frames_indexed

| Coluna | Tipo | Origem |
|--------|------|--------|
| match_id | STRING | PK |
| participant_id | INT | PK — 1-10 original |
| frame_index | INT | PK — ROW_NUMBER sequencial |
| puuid | STRING | match_participants (JOIN) |
| champion_name | STRING | match_participants (JOIN) |
| timestamp_ms | LONG | timeline_frames |
| minute | INT | timeline_frames |
| current_gold | INT | timeline_frames |
| total_gold | INT | timeline_frames |
| xp | INT | timeline_frames |
| level | INT | timeline_frames |
| minions_killed | INT | timeline_frames |
| jungle_minions_killed | INT | timeline_frames |
| position_x | INT | timeline_frames |
| position_y | INT | timeline_frames |

**PK:** (match_id, participant_id, frame_index)
**JOIN:** `silver.timeline_frames tf JOIN silver.match_participants mp ON tf.match_id = mp.match_id AND tf.participant_id = mp.participant_id`
**frame_index:** `ROW_NUMBER() OVER(PARTITION BY match_id, participant_id ORDER BY timestamp_ms)`
**Filtro:** matches com game_duration_seconds < 300 excluidos (JOIN silver.matches)

**IMPORTANTE:** Requer pre-requisito (participant_id na silver_match_participants).

**Validacoes:**
- frame_index sequencial sem gaps por participant por match
- 10 participants por match
- participant_id entre 1 e 10

## Design

Cada notebook segue o padrao silver existente:

```python
# 1. Leitura das tabelas silver
df = spark.read.table("loldata.cblol_silver.<tabela>")

# 2. JOINs entre tabelas silver
df_joined = df.join(df2, on="match_id")

# 3. Transformacao (colunas calculadas, filtros, window functions)
df_gold = df_joined.select(...)

# 4. Dedup por Window + row_number (padrao KB: DELTA_MULTIPLE_SOURCE_ROW_MATCHING)
# Necessario quando o source pode ter multiplas linhas por PK (ex: extraction_date duplicada)

# 5. Escrita com MERGE (idempotente)
# CREATE TABLE IF NOT EXISTS + MERGE INTO usando PKs
```

## Testes

### Assertions inline (em cada notebook)
- `count > 0` — tabela nao vazia
- `null check PKs` — chaves primarias nunca nulas
- `no duplicates` — PKs distintas == contagem total
- `game_duration_seconds >= 300` — nenhum remake presente
- `team_id in (100, 200)` — valores validos

### Notebook de validacao referencial (gold_validation.py)
1. Todo match_id em gold_team_stats existe em silver.matches
2. Todo match_id em gold_player_performance existe em silver.matches
3. Todo puuid em gold_player_performance existe em silver.accounts
4. Contagem de linhas por match_id em gold_team_stats == 2
5. Contagem de linhas por match_id em gold_player_performance == 10
6. Contagem de linhas por match_id em gold_draft BETWEEN 10 AND 20
7. win em gold_team_stats coerente com winning_team_id em silver.matches
8. Metricas per-minute >= 0 em gold_player_performance

## Criterios de aceite

| ID | Criterio | Validacao |
|----|----------|-----------|
| SC-001 | Schema loldata.cblol_gold criado | SHOW TABLES IN loldata.cblol_gold |
| SC-002 | 4 tabelas gold + 1 validacao criados | 5 notebooks em databricks/gold/ |
| SC-003 | Nenhum remake (duration < 300s) | WHERE game_duration_seconds < 300 COUNT = 0 |
| SC-004 | MERGE idempotente | Re-rodar nao duplica linhas |
| SC-005 | PKs nao-nulas e unicas | ASSERT em cada notebook |
| SC-006 | Metricas calculadas corretas | Spot check manual de 3 partidas |
| SC-007 | participant_id adicionado a silver | SELECT participant_id FROM silver.match_participants LIMIT 1 |

## Ordem de execucao

1. Amend silver_match_participants (adicionar + popular participant_id)
2. CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold
3. gold_team_stats.py (independente)
4. gold_player_performance.py (depende de accounts)
5. gold_draft.py (depende de champion mapping via match_participants)
6. gold_player_frames_indexed.py (depende de participant_id)
7. gold_validation.py (todas as assertions)

## Arquivos a criar

```
databricks/
  silver/
    silver_match_participants_amend.py   <-- adiciona participant_id
  gold/
    gold_team_stats.py
    gold_player_performance.py
    gold_draft.py
    gold_player_frames_indexed.py
    gold_validation.py
```
