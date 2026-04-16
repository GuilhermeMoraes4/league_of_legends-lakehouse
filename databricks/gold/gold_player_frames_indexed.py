# Databricks notebook source
# Notebook : gold_player_frames_indexed
# Pre-req  : silver_match_participants_amend.py executado (participant_id populado)
# Origem   : loldata.cblol_silver.timeline_frames + match_participants + matches
# Destino  : loldata.cblol_gold.gold_player_frames_indexed
# PK       : match_id + participant_id + frame_index
# Descricao: Frames de timeline indexados sequencialmente por participante por
#            partida. frame_index = ROW_NUMBER por (match_id, participant_id)
#            ordenado por timestamp_ms. Exclui remakes (< 300s).

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_frames = spark.read.table("loldata.cblol_silver.timeline_frames")
df_participants = spark.read.table("loldata.cblol_silver.match_participants")
df_matches = spark.read.table("loldata.cblol_silver.matches")

df_valid_matches = df_matches.filter(col("game_duration_seconds") >= 300).select("match_id")

# Dedup participants por (match_id, participant_id): pega a ingestao mais recente
w_p = Window.partitionBy("match_id", "participant_id").orderBy(col("extraction_date").desc())
df_participants_slim = (
    df_participants
    .select("match_id", "participant_id", "puuid", "champion_name", "extraction_date")
    .withColumn("_rn", row_number().over(w_p))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .select("match_id", "participant_id", "puuid", "champion_name")
)

# COMMAND ----------

# JOIN frames com partidas validas e com o mapa participant -> puuid/champion_name
df_joined = (
    df_frames
    .join(df_valid_matches, on="match_id")
    .join(df_participants_slim, on=["match_id", "participant_id"], how="left")
)

# frame_index sequencial por (match_id, participant_id) ordenado por timestamp_ms
w_frame = Window.partitionBy("match_id", "participant_id").orderBy("timestamp_ms")

df_gold = df_joined.withColumn("frame_index", row_number().over(w_frame)).select(
    col("match_id"),
    col("participant_id"),
    col("frame_index"),
    col("puuid"),
    col("champion_name"),
    col("timestamp_ms"),
    col("minute"),
    col("current_gold"),
    col("total_gold"),
    col("xp"),
    col("level"),
    col("minions_killed"),
    col("jungle_minions_killed"),
    col("position_x"),
    col("position_y"),
    col("extraction_date"),
)

# COMMAND ----------

# Dedup por PK antes do MERGE
w_dedup = Window.partitionBy("match_id", "participant_id", "frame_index").orderBy(
    col("extraction_date").desc()
)
df_deduped = (
    df_gold
    .withColumn("_rn", row_number().over(w_dedup))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .drop("extraction_date")
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_player_frames_indexed (
        match_id                STRING,
        participant_id          INT,
        frame_index             INT,
        puuid                   STRING,
        champion_name           STRING,
        timestamp_ms            LONG,
        minute                  INT,
        current_gold            INT,
        total_gold              INT,
        xp                      INT,
        level                   INT,
        minions_killed          INT,
        jungle_minions_killed   INT,
        position_x              INT,
        position_y              INT
    )
    USING DELTA
    COMMENT 'Frames de timeline indexados por participante por partida (sem remakes)'
    """
)

# COMMAND ----------

df_deduped.createOrReplaceTempView("source")

spark.sql(
    """
    MERGE INTO loldata.cblol_gold.gold_player_frames_indexed t
    USING source s
    ON t.match_id = s.match_id
    AND t.participant_id = s.participant_id
    AND t.frame_index = s.frame_index
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_gold.gold_player_frames_indexed")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(
    col("match_id").isNull()
    | col("participant_id").isNull()
    | col("frame_index").isNull()
).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com PK nula"

total = df_final.count()
distinct = (
    df_final.select("match_id", "participant_id", "frame_index").distinct().count()
)
assert total == distinct, (
    f"FALHA: duplicatas em gold_player_frames_indexed (total={total}, distinct={distinct})"
)

invalid_pid = df_final.filter(
    (col("participant_id") < 1) | (col("participant_id") > 10)
).count()
assert invalid_pid == 0, (
    f"FALHA: {invalid_pid} linhas com participant_id fora de [1, 10]"
)

print(
    f"OK: loldata.cblol_gold.gold_player_frames_indexed — {count} registros carregados"
)
