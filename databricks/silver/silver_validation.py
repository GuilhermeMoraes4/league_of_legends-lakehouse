# Databricks notebook source
# Notebook : silver_validation
# Descricao: Valida integridade referencial entre todas as tabelas da camada
#            silver. Deve ser executado apos todos os notebooks de
#            transformacao (silver_accounts, silver_matches, silver_match_teams,
#            silver_match_participants, silver_timeline_frames,
#            silver_timeline_events).
#
# Checks executados:
#   1. Todo match_id em match_teams         existe em matches
#   2. Todo match_id em match_participants  existe em matches
#   3. Todo puuid   em match_participants   existe em accounts
#   4. Todo match_id em timeline_frames     existe em matches
#   5. Todo match_id em timeline_events     existe em matches
#   6. Contagem de participantes por partida == 10
#   7. Contagem de times por partida        == 2

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct

# COMMAND ----------

# Leitura das tabelas silver
accounts        = spark.read.table("loldata.cblol_silver.accounts")
matches         = spark.read.table("loldata.cblol_silver.matches")
match_teams     = spark.read.table("loldata.cblol_silver.match_teams")
match_parts     = spark.read.table("loldata.cblol_silver.match_participants")
tl_frames       = spark.read.table("loldata.cblol_silver.timeline_frames")
tl_events       = spark.read.table("loldata.cblol_silver.timeline_events")

# Conjuntos de chaves de referencia (cache para reutilizar em multiplos joins)
match_ids_ref   = matches.select("match_id")
account_puuids  = accounts.select("puuid")

# COMMAND ----------
# --- CHECK 1: Todo match_id em match_teams existe em matches ---

orphan_teams = (
    match_teams
    .select("match_id")
    .distinct()
    .join(match_ids_ref, on="match_id", how="left_anti")
    .count()
)
assert orphan_teams == 0, (
    f"FALHA [check 1]: {orphan_teams} match_id(s) em match_teams sem registro em matches"
)
print("OK [check 1]: todos os match_id de match_teams existem em matches")

# COMMAND ----------
# --- CHECK 2: Todo match_id em match_participants existe em matches ---

orphan_parts = (
    match_parts
    .select("match_id")
    .distinct()
    .join(match_ids_ref, on="match_id", how="left_anti")
    .count()
)
assert orphan_parts == 0, (
    f"FALHA [check 2]: {orphan_parts} match_id(s) em match_participants sem registro em matches"
)
print("OK [check 2]: todos os match_id de match_participants existem em matches")

# COMMAND ----------
# --- CHECK 3: Todo puuid em match_participants existe em accounts ---

orphan_puuids = (
    match_parts
    .select("puuid")
    .distinct()
    .join(account_puuids, on="puuid", how="left_anti")
    .count()
)
assert orphan_puuids == 0, (
    f"FALHA [check 3]: {orphan_puuids} puuid(s) em match_participants sem registro em accounts"
)
print("OK [check 3]: todos os puuid de match_participants existem em accounts")

# COMMAND ----------
# --- CHECK 4: Todo match_id em timeline_frames existe em matches ---

orphan_tf = (
    tl_frames
    .select("match_id")
    .distinct()
    .join(match_ids_ref, on="match_id", how="left_anti")
    .count()
)
assert orphan_tf == 0, (
    f"FALHA [check 4]: {orphan_tf} match_id(s) em timeline_frames sem registro em matches"
)
print("OK [check 4]: todos os match_id de timeline_frames existem em matches")

# COMMAND ----------
# --- CHECK 5: Todo match_id em timeline_events existe em matches ---

orphan_te = (
    tl_events
    .select("match_id")
    .distinct()
    .join(match_ids_ref, on="match_id", how="left_anti")
    .count()
)
assert orphan_te == 0, (
    f"FALHA [check 5]: {orphan_te} match_id(s) em timeline_events sem registro em matches"
)
print("OK [check 5]: todos os match_id de timeline_events existem em matches")

# COMMAND ----------
# --- CHECK 6: Contagem de participantes por partida == 10 ---

# Partidas com numero de participantes diferente de 10 (exceto ARAM e modos especiais
# que podem ter menos; aqui verificamos apenas partidas presentes em ambas as tabelas)
bad_participant_counts = (
    match_parts
    .groupBy("match_id")
    .agg(count("*").alias("n_participants"))
    .filter(col("n_participants") != 10)
    .count()
)
assert bad_participant_counts == 0, (
    f"FALHA [check 6]: {bad_participant_counts} partida(s) com numero de participantes != 10"
)
print("OK [check 6]: todas as partidas tem exatamente 10 participantes")

# COMMAND ----------
# --- CHECK 7: Contagem de times por partida == 2 ---

bad_team_counts = (
    match_teams
    .groupBy("match_id")
    .agg(count("*").alias("n_teams"))
    .filter(col("n_teams") != 2)
    .count()
)
assert bad_team_counts == 0, (
    f"FALHA [check 7]: {bad_team_counts} partida(s) com numero de times != 2"
)
print("OK [check 7]: todas as partidas tem exatamente 2 times")

# COMMAND ----------

# Sumario final
print("\n=== VALIDACAO SILVER CONCLUIDA COM SUCESSO ===")
print(f"  accounts         : {accounts.count()} jogadores")
print(f"  matches          : {matches.count()} partidas")
print(f"  match_teams      : {match_teams.count()} registros")
print(f"  match_participants: {match_parts.count()} registros")
print(f"  timeline_frames  : {tl_frames.count()} registros")
print(f"  timeline_events  : {tl_events.count()} registros")
