# Databricks notebook source
# Notebook : gold_validation
# Descricao: Validacao referencial entre as tabelas gold e silver.
#            Executar apos todos os notebooks gold estarem carregados.

# COMMAND ----------

df_gts = spark.read.table("loldata.cblol_gold.gold_team_stats")
df_gpp = spark.read.table("loldata.cblol_gold.gold_player_performance")
df_gd  = spark.read.table("loldata.cblol_gold.gold_draft")
df_gpf = spark.read.table("loldata.cblol_gold.gold_player_frames_indexed")
df_sm  = spark.read.table("loldata.cblol_silver.matches")
df_sa  = spark.read.table("loldata.cblol_silver.accounts")
df_smp = spark.read.table("loldata.cblol_silver.match_participants")

# COMMAND ----------

# 1. Todo match_id em gold_team_stats existe em silver.matches
orphan_gts = (
    df_gts.select("match_id").distinct()
    .join(df_sm.select("match_id").distinct(), on="match_id", how="left_anti")
    .count()
)
assert orphan_gts == 0, (
    f"FALHA: {orphan_gts} match_ids em gold_team_stats sem correspondencia em silver.matches"
)

# 2. Todo match_id em gold_player_performance existe em silver.matches
orphan_gpp_match = (
    df_gpp.select("match_id").distinct()
    .join(df_sm.select("match_id").distinct(), on="match_id", how="left_anti")
    .count()
)
assert orphan_gpp_match == 0, (
    f"FALHA: {orphan_gpp_match} match_ids em gold_player_performance sem correspondencia em silver.matches"
)

# 3. Todo puuid em gold_player_performance existe em silver.accounts
orphan_puuid = (
    df_gpp.select("puuid").distinct()
    .join(df_sa.select("puuid").distinct(), on="puuid", how="left_anti")
    .count()
)
assert orphan_puuid == 0, (
    f"FALHA: {orphan_puuid} puuids em gold_player_performance sem correspondencia em silver.accounts"
)

# COMMAND ----------

# 4. Exatamente 2 linhas por match em gold_team_stats
bad_team_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_team_stats
        GROUP BY match_id HAVING n != 2
    )
    """
).collect()[0][0]
assert bad_team_counts == 0, (
    f"FALHA: {bad_team_counts} partidas com count != 2 em gold_team_stats"
)

# 5. Exatamente 10 linhas por match em gold_player_performance
bad_player_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_player_performance
        GROUP BY match_id HAVING n != 10
    )
    """
).collect()[0][0]
assert bad_player_counts == 0, (
    f"FALHA: {bad_player_counts} partidas com count != 10 em gold_player_performance"
)

# 6. Entre 10 e 20 linhas por match em gold_draft
bad_draft_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_draft
        GROUP BY match_id HAVING n < 10 OR n > 20
    )
    """
).collect()[0][0]
assert bad_draft_counts == 0, (
    f"FALHA: {bad_draft_counts} partidas com count fora de [10, 20] em gold_draft"
)

# COMMAND ----------

# 7. win coerente com winning_team_id
bad_win = spark.sql(
    """
    SELECT COUNT(*) FROM loldata.cblol_gold.gold_team_stats t
    JOIN loldata.cblol_silver.matches m ON t.match_id = m.match_id
    WHERE t.team_id = m.winning_team_id AND t.win = false
    """
).collect()[0][0]
assert bad_win == 0, (
    f"FALHA: {bad_win} linhas onde time vencedor (team_id = winning_team_id) tem win = false"
)

# 8. Metricas per-minute >= 0 em gold_player_performance
negative_metrics = spark.sql(
    """
    SELECT COUNT(*) FROM loldata.cblol_gold.gold_player_performance
    WHERE gold_per_minute < 0
       OR damage_per_minute < 0
       OR cs_per_minute < 0
       OR damage_per_gold < 0
    """
).collect()[0][0]
assert negative_metrics == 0, (
    f"FALHA: {negative_metrics} linhas com metricas per-minute negativas em gold_player_performance"
)

# 9. participant_id em gold_player_frames_indexed existe em silver.match_participants
orphan_pid = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT DISTINCT gpf.match_id, gpf.participant_id
        FROM loldata.cblol_gold.gold_player_frames_indexed gpf
        LEFT ANTI JOIN (
            SELECT DISTINCT match_id, participant_id
            FROM loldata.cblol_silver.match_participants
            WHERE participant_id IS NOT NULL
        ) smp
        ON gpf.match_id = smp.match_id AND gpf.participant_id = smp.participant_id
    )
    """
).collect()[0][0]
assert orphan_pid == 0, (
    f"FALHA: {orphan_pid} (match_id, participant_id) em gold_player_frames_indexed "
    f"sem correspondencia em silver.match_participants"
)

# COMMAND ----------

print("OK: todas as validacoes referenciais gold passaram")
