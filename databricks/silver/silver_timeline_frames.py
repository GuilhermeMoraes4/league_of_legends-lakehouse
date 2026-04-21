# Databricks notebook source
# Notebook : silver_timeline_frames
# Origem   : loldata.cblol_bronze.timelines  (frames[].participantFrames)
# Destino  : loldata.cblol_silver.timeline_frames
# PK       : match_id + participant_id + timestamp_ms
# Descricao: Explode os frames de timeline por jogador por minuto.
#            Cada frame possui posicao, ouro, XP, nivel e minions de cada
#            participante. O campo minute eh calculado como floor(timestamp_ms / 60000).
#            participantFrames eh um MAP<STRING, STRUCT> onde as chaves sao
#            "1", "2", ..., "10" (participant IDs como string).

# COMMAND ----------

from pyspark.sql.functions import col, explode, floor, from_json, map_values, row_number
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------

# Schema da posicao de cada participante dentro de participantFrames
schema_position = StructType(
    [
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
    ]
)

# Schema de cada participantFrame (indexado por participantId como chave do mapa)
schema_participant_frame = StructType(
    [
        StructField("participantId", IntegerType(), True),
        StructField("currentGold", IntegerType(), True),
        StructField("totalGold", IntegerType(), True),
        StructField("xp", IntegerType(), True),
        StructField("level", IntegerType(), True),
        StructField("minionsKilled", IntegerType(), True),
        StructField("jungleMinionsKilled", IntegerType(), True),
        StructField("position", schema_position, True),
    ]
)

# Schema de cada frame (um por minuto)
schema_frame = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField(
            "participantFrames",
            MapType(StringType(), schema_participant_frame),
            True,
        ),
    ]
)

# Schema do campo info dentro do JSON de timeline
schema_info = StructType(
    [
        StructField("frames", ArrayType(schema_frame), True),
    ]
)

# Schema raiz do JSON de timeline (Match-V5 timeline)
schema_timeline = StructType(
    [
        StructField(
            "metadata",
            StructType([StructField("matchId", StringType(), True)]),
            True,
        ),
        StructField("info", schema_info, True),
    ]
)

# COMMAND ----------

# Leitura da camada bronze
df_bronze = spark.read.table("loldata.cblol_bronze.timelines")

# Parse do JSON com schema explicito
df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_timeline))

# Explode dos frames: 1 linha por frame (1 por minuto) por partida
df_frames = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.frames")).alias("frame"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# Explode do mapa participantFrames: 1 linha por participante por frame
# map_values() extrai os valores do mapa (os structs de cada participante)
df_participant_frames = df_frames.select(
    col("match_id"),
    col("frame.timestamp").alias("timestamp_ms"),
    explode(map_values(col("frame.participantFrames"))).alias("pf"),
    col("extraction_date"),
)

# Selecao dos campos finais e calculo do minute
df_silver = df_participant_frames.select(
    col("match_id"),
    col("pf.participantId").alias("participant_id"),
    col("timestamp_ms"),
    floor(col("timestamp_ms") / 60000).cast(IntegerType()).alias("minute"),
    col("pf.currentGold").alias("current_gold"),
    col("pf.totalGold").alias("total_gold"),
    col("pf.xp").alias("xp"),
    col("pf.level").alias("level"),
    col("pf.minionsKilled").alias("minions_killed"),
    col("pf.jungleMinionsKilled").alias("jungle_minions_killed"),
    col("pf.position.x").alias("position_x"),
    col("pf.position.y").alias("position_y"),
    col("extraction_date"),
).filter(col("participant_id").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.timeline_frames (
        match_id                STRING,
        participant_id          INT,
        timestamp_ms            LONG,
        minute                  INT,
        current_gold            INT,
        total_gold              INT,
        xp                      INT,
        level                   INT,
        minions_killed          INT,
        jungle_minions_killed   INT,
        position_x              INT,
        position_y              INT,
        extraction_date         DATE
    )
    USING DELTA
    COMMENT 'Um registro por participante por minuto por partida — dados de frame da timeline (Match-V5)'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
# Deduplicacao: manter apenas a extracao mais recente por PK
window_dedup = Window.partitionBy("match_id", "participant_id", "timestamp_ms").orderBy(col("extraction_date").desc())
df_silver = df_silver.withColumn("rn", row_number().over(window_dedup)).filter(col("rn") == 1).drop("rn")

df_silver.createOrReplaceTempView("timeline_frames_updates")

# MERGE idempotente: upsert por (match_id, participant_id, timestamp_ms)
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.timeline_frames AS target
    USING timeline_frames_updates AS source
    ON target.match_id       = source.match_id
    AND target.participant_id = source.participant_id
    AND target.timestamp_ms  = source.timestamp_ms
    WHEN MATCHED THEN
        UPDATE SET
            target.minute                 = source.minute,
            target.current_gold           = source.current_gold,
            target.total_gold             = source.total_gold,
            target.xp                     = source.xp,
            target.level                  = source.level,
            target.minions_killed         = source.minions_killed,
            target.jungle_minions_killed  = source.jungle_minions_killed,
            target.position_x             = source.position_x,
            target.position_y             = source.position_y,
            target.extraction_date        = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (
            match_id, participant_id, timestamp_ms, minute,
            current_gold, total_gold, xp, level,
            minions_killed, jungle_minions_killed,
            position_x, position_y, extraction_date
        )
        VALUES (
            source.match_id, source.participant_id, source.timestamp_ms, source.minute,
            source.current_gold, source.total_gold, source.xp, source.level,
            source.minions_killed, source.jungle_minions_killed,
            source.position_x, source.position_y, source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.timeline_frames")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

# Chaves primarias nunca nulas
null_match_id = df_final.filter(col("match_id").isNull()).count()
assert null_match_id == 0, f"FALHA: {null_match_id} linhas com match_id nulo"

null_participant_id = df_final.filter(col("participant_id").isNull()).count()
assert null_participant_id == 0, (
    f"FALHA: {null_participant_id} linhas com participant_id nulo"
)

null_timestamp = df_final.filter(col("timestamp_ms").isNull()).count()
assert null_timestamp == 0, f"FALHA: {null_timestamp} linhas com timestamp_ms nulo"

# Sem duplicatas na PK composta (match_id, participant_id, timestamp_ms)
total = df_final.count()
distinct = (
    df_final.select("match_id", "participant_id", "timestamp_ms").distinct().count()
)
assert total == distinct, (
    f"FALHA: duplicatas em timeline_frames (total={total}, distinct={distinct})"
)

# minute nunca negativo
negative_minute = df_final.filter(col("minute") < 0).count()
assert negative_minute == 0, f"FALHA: {negative_minute} linhas com minute negativo"

print(f"OK: loldata.cblol_silver.timeline_frames — {count} registros carregados")
