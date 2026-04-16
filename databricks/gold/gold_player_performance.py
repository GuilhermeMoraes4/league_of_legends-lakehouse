# Databricks notebook source
# Notebook : gold_player_performance
# Origem   : loldata.cblol_silver.match_participants + matches + accounts
# Destino  : loldata.cblol_gold.gold_player_performance
# PK       : match_id + puuid
# Descricao: Performance individual por jogador por partida. Calcula GPM, DPM,
#            CSPM e damage_per_gold. KDA herdado da silver (nao recalculado).
#            Exclui remakes (game_duration_seconds < 300).

# COMMAND ----------

from pyspark.sql.functions import col, greatest, lit, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_participants = spark.read.table("loldata.cblol_silver.match_participants")
df_matches = spark.read.table("loldata.cblol_silver.matches")
df_accounts = spark.read.table("loldata.cblol_silver.accounts")

df_matches_filtered = df_matches.filter(col("game_duration_seconds") >= 300).select(
    "match_id", "game_duration_seconds"
)

df_accounts_slim = df_accounts.select("puuid", "game_name", "tag_line", "team")

df_joined = (
    df_participants
    .join(df_matches_filtered, on="match_id")
    .join(df_accounts_slim, on="puuid")
)

# COMMAND ----------

minutes = col("game_duration_seconds") / lit(60.0)
total_cs = col("total_minions_killed") + col("neutral_minions_killed")

df_gold = df_joined.select(
    col("match_id"),
    col("puuid"),
    col("game_name"),
    col("tag_line"),
    col("team"),
    col("team_id"),
    col("champion_name"),
    col("champion_id"),
    col("role"),
    col("individual_position"),
    col("kills"),
    col("deaths"),
    col("assists"),
    col("kda"),
    col("gold_earned"),
    col("total_minions_killed"),
    col("neutral_minions_killed"),
    total_cs.alias("total_cs"),
    col("vision_score"),
    col("wards_placed"),
    col("wards_killed"),
    col("total_damage_dealt_to_champions"),
    col("total_damage_taken"),
    col("total_heal"),
    (col("gold_earned") / minutes).alias("gold_per_minute"),
    (col("total_damage_dealt_to_champions") / minutes).alias("damage_per_minute"),
    (total_cs / minutes).alias("cs_per_minute"),
    (col("total_damage_dealt_to_champions") / greatest(col("gold_earned"), lit(1))).alias("damage_per_gold"),
    col("item0"),
    col("item1"),
    col("item2"),
    col("item3"),
    col("item4"),
    col("item5"),
    col("item6"),
    col("summoner1_id"),
    col("summoner2_id"),
    col("win"),
    col("game_duration_seconds"),
    col("extraction_date"),
)

# COMMAND ----------

# Dedup por PK antes do MERGE
w = Window.partitionBy("match_id", "puuid").orderBy(col("extraction_date").desc())
df_deduped = (
    df_gold
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .drop("extraction_date")
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_player_performance (
        match_id                            STRING,
        puuid                               STRING,
        game_name                           STRING,
        tag_line                            STRING,
        team                                STRING,
        team_id                             INT,
        champion_name                       STRING,
        champion_id                         INT,
        role                                STRING,
        individual_position                 STRING,
        kills                               INT,
        deaths                              INT,
        assists                             INT,
        kda                                 DOUBLE,
        gold_earned                         INT,
        total_minions_killed                INT,
        neutral_minions_killed              INT,
        total_cs                            INT,
        vision_score                        INT,
        wards_placed                        INT,
        wards_killed                        INT,
        total_damage_dealt_to_champions     INT,
        total_damage_taken                  INT,
        total_heal                          INT,
        gold_per_minute                     DOUBLE,
        damage_per_minute                   DOUBLE,
        cs_per_minute                       DOUBLE,
        damage_per_gold                     DOUBLE,
        item0                               INT,
        item1                               INT,
        item2                               INT,
        item3                               INT,
        item4                               INT,
        item5                               INT,
        item6                               INT,
        summoner1_id                        INT,
        summoner2_id                        INT,
        win                                 BOOLEAN,
        game_duration_seconds               INT
    )
    USING DELTA
    COMMENT 'Performance individual por jogador por partida — metricas calculadas (sem remakes)'
    """
)

# COMMAND ----------

df_deduped.createOrReplaceTempView("source")

spark.sql(
    """
    MERGE INTO loldata.cblol_gold.gold_player_performance t
    USING source s
    ON t.match_id = s.match_id AND t.puuid = s.puuid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_gold.gold_player_performance")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(
    col("match_id").isNull() | col("puuid").isNull()
).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com PK nula"

total = df_final.count()
distinct = df_final.select("match_id", "puuid").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em gold_player_performance (total={total}, distinct={distinct})"
)

bad_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_player_performance
        GROUP BY match_id HAVING n != 10
    )
    """
).collect()[0][0]
assert bad_counts == 0, (
    f"FALHA: {bad_counts} partidas sem exatamente 10 linhas em gold_player_performance"
)

negative_metrics = df_final.filter(
    (col("gold_per_minute") < 0)
    | (col("damage_per_minute") < 0)
    | (col("cs_per_minute") < 0)
    | (col("damage_per_gold") < 0)
).count()
assert negative_metrics == 0, (
    f"FALHA: {negative_metrics} linhas com metricas per-minute negativas"
)

print(f"OK: loldata.cblol_gold.gold_player_performance — {count} registros carregados")
