# Databricks notebook source
# Notebook : gold_team_stats
# Origem   : loldata.cblol_silver.match_teams + loldata.cblol_silver.matches
# Destino  : loldata.cblol_gold.gold_team_stats
# PK       : match_id + team_id
# Descricao: Estatisticas por time por partida — objetivos e resultado.
#            Exclui remakes (game_duration_seconds < 300).

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_teams = spark.read.table("loldata.cblol_silver.match_teams")
df_matches = spark.read.table("loldata.cblol_silver.matches")

df_matches_filtered = df_matches.filter(col("game_duration_seconds") >= 300).select(
    "match_id", "game_duration_seconds", "game_creation", "winning_team_id"
)

df_joined = df_teams.join(df_matches_filtered, on="match_id").select(
    col("match_id"),
    col("team_id"),
    col("win"),
    col("baron_kills"),
    col("dragon_kills"),
    col("tower_kills"),
    col("inhibitor_kills"),
    col("rift_herald_kills"),
    col("game_duration_seconds"),
    col("game_creation"),
    col("winning_team_id"),
    col("extraction_date"),
)

# COMMAND ----------

# Dedup por PK antes do MERGE
w = Window.partitionBy("match_id", "team_id").orderBy(col("extraction_date").desc())
df_deduped = (
    df_joined
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .drop("extraction_date")
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_team_stats (
        match_id                STRING,
        team_id                 INT,
        win                     BOOLEAN,
        baron_kills             INT,
        dragon_kills            INT,
        tower_kills             INT,
        inhibitor_kills         INT,
        rift_herald_kills       INT,
        game_duration_seconds   INT,
        game_creation           TIMESTAMP,
        winning_team_id         INT
    )
    USING DELTA
    COMMENT 'Stats por time por partida — objetivos e resultado (sem remakes)'
    """
)

# COMMAND ----------

df_deduped.createOrReplaceTempView("source")

spark.sql(
    """
    MERGE INTO loldata.cblol_gold.gold_team_stats t
    USING source s
    ON t.match_id = s.match_id AND t.team_id = s.team_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_gold.gold_team_stats")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(
    col("match_id").isNull() | col("team_id").isNull()
).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com PK nula"

total = df_final.count()
distinct = df_final.select("match_id", "team_id").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em gold_team_stats (total={total}, distinct={distinct})"
)

bad_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_team_stats
        GROUP BY match_id HAVING n != 2
    )
    """
).collect()[0][0]
assert bad_counts == 0, (
    f"FALHA: {bad_counts} partidas sem exatamente 2 linhas em gold_team_stats"
)

print(f"OK: loldata.cblol_gold.gold_team_stats — {count} registros carregados")
