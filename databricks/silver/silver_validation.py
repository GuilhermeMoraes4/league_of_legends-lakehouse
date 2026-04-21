# Databricks notebook source
# Notebook : silver_validation
# Descricao: Validacao referencial cruzada entre as 6 tabelas silver.
#            Verifica integridade referencial, contagens esperadas e ausencia
#            de orfaos. Deve ser executado apos todos os notebooks de carga.

# COMMAND ----------

from pyspark.sql.functions import col, count

# COMMAND ----------

# Carrega todas as tabelas silver para validacao
accounts         = spark.read.table("loldata.cblol_silver.accounts")
matches          = spark.read.table("loldata.cblol_silver.matches")
match_teams      = spark.read.table("loldata.cblol_silver.match_teams")
match_parts      = spark.read.table("loldata.cblol_silver.match_participants")
timeline_frames  = spark.read.table("loldata.cblol_silver.timeline_frames")
timeline_events  = spark.read.table("loldata.cblol_silver.timeline_events")

erros = []

# COMMAND ----------

# --- Validacao 1: match_teams sem registro orfao em matches ---
orphan_teams = match_teams.join(
    matches.select("match_id"), on="match_id", how="left_anti"
).count()
if orphan_teams > 0:
    erros.append(
        f"FALHA V1: {orphan_teams} match_id em match_teams sem correspondencia em matches"
    )
else:
    print("OK V1: todos os match_id de match_teams existem em matches")

# COMMAND ----------

# --- Validacao 2: match_participants sem registro orfao em matches ---
orphan_parts = match_parts.join(
    matches.select("match_id"), on="match_id", how="left_anti"
).count()
if orphan_parts > 0:
    erros.append(
        f"FALHA V2: {orphan_parts} match_id em match_participants sem correspondencia em matches"
    )
else:
    print("OK V2: todos os match_id de match_participants existem em matches")

# COMMAND ----------

# --- Validacao 3: puuid de match_participants existe em accounts ---
orphan_puuids = match_parts.join(
    accounts.select("puuid"), on="puuid", how="left_anti"
).count()
if orphan_puuids > 0:
    erros.append(
        f"FALHA V3: {orphan_puuids} puuid em match_participants sem correspondencia em accounts"
    )
else:
    print("OK V3: todos os puuid de match_participants existem em accounts")

# COMMAND ----------

# --- Validacao 4: timeline_frames sem registro orfao em matches ---
orphan_frames = timeline_frames.join(
    matches.select("match_id"), on="match_id", how="left_anti"
).count()
if orphan_frames > 0:
    erros.append(
        f"FALHA V4: {orphan_frames} match_id em timeline_frames sem correspondencia em matches"
    )
else:
    print("OK V4: todos os match_id de timeline_frames existem em matches")

# COMMAND ----------

# --- Validacao 5: timeline_events sem registro orfao em matches ---
orphan_events = timeline_events.join(
    matches.select("match_id"), on="match_id", how="left_anti"
).count()
if orphan_events > 0:
    erros.append(
        f"FALHA V5: {orphan_events} match_id em timeline_events sem correspondencia em matches"
    )
else:
    print("OK V5: todos os match_id de timeline_events existem em matches")

# COMMAND ----------

# --- Validacao 6: cada partida deve ter exatamente 10 participantes ---
counts_per_match = match_parts.groupBy("match_id").agg(
    count("puuid").alias("n_participants")
)
wrong_count = counts_per_match.filter(col("n_participants") != 10).count()
if wrong_count > 0:
    samples = (
        counts_per_match.filter(col("n_participants") != 10).limit(5).collect()
    )
    erros.append(
        f"FALHA V6: {wrong_count} partidas sem exatamente 10 participantes. "
        f"Exemplos: {samples}"
    )
else:
    total_matches = counts_per_match.count()
    print(f"OK V6: todas as {total_matches} partidas tem exatamente 10 participantes")

# COMMAND ----------

# --- Validacao 7: cada partida deve ter exatamente 2 times ---
counts_teams = match_teams.groupBy("match_id").agg(
    count("team_id").alias("n_teams")
)
wrong_teams = counts_teams.filter(col("n_teams") != 2).count()
if wrong_teams > 0:
    samples = (
        counts_teams.filter(col("n_teams") != 2).limit(5).collect()
    )
    erros.append(
        f"FALHA V7: {wrong_teams} partidas sem exatamente 2 times. "
        f"Exemplos: {samples}"
    )
else:
    total_matches_teams = counts_teams.count()
    print(f"OK V7: todas as {total_matches_teams} partidas tem exatamente 2 times")

# COMMAND ----------

# Sumario final
print("\n--- SUMARIO DE VALIDACAO SILVER ---")
print(f"accounts           : {accounts.count()} linhas")
print(f"matches            : {matches.count()} linhas")
print(f"match_teams        : {match_teams.count()} linhas")
print(f"match_participants : {match_parts.count()} linhas")
print(f"timeline_frames    : {timeline_frames.count()} linhas")
print(f"timeline_events    : {timeline_events.count()} linhas")

if erros:
    print(f"\n{len(erros)} FALHA(S) ENCONTRADA(S):")
    for e in erros:
        print(f"  - {e}")
    raise AssertionError(
        f"Validacao silver falhou com {len(erros)} erro(s). Ver log acima."
    )
else:
    print("\nTODAS AS VALIDACOES PASSARAM.")
