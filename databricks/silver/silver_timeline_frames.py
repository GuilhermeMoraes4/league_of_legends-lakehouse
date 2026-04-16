# Databricks notebook source
# Notebook : silver_timeline_frames
# Origem   : loldata.cblol_bronze.timelines  (info.frames[].participantFrames{})
# Destino  : loldata.cblol_silver.timeline_frames
# PK       : match_id + participant_id + timestamp_ms
# Descricao: Explode o array info.frames[] e depois o mapa participantFrames
#            para uma linha por jogador por minuto por partida.
#            Calcula minute = floor(timestamp_ms / 60000).

# COMMAND ----------

from pyspark.sql.functions import col, explode, floor, from_json
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# Schema da posicao no mapa
schema_position = StructType(
    [
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
    ]
)

# Schema de um participantFrame individual
# (campos relevantes para a camada silver — campos de stats avancados sao omitidos)
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

# Schema de um frame de timeline
# participantFrames eh um mapa com chaves "1".."10" (string) e valor struct
# events[] e lido no notebook silver_timeline_events
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

schema_metadata = StructType(
    [
        StructField("matchId", StringType(), True),
    ]
)

schema_info = StructType(
    [
        StructField("frames", ArrayType(schema_frame), True),
    ]
)

# Schema raiz do JSON de timeline (Match-V5 timeline)
schema_timeline = StructType(
    [
        StructField("metadata", schema_metadata, True),
        StructField("info", schema_info, True),
    ]
)

# COMMAND ----------

# Leitura da camada bronze
df_bronze = spark.read.table("loldata.cblol_bronze.timelines")

# Parse do JSON com schema explicito
df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_timeline))

# Explode 1: info.frames[] -> uma linha por frame (minuto) por partida
df_frames = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.frames")).alias("frame"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# Explode 2: participantFrames{} -> uma linha por jogador por frame
# explode em MapType retorna duas colunas: chave e valor
df_pf = df_frames.select(
    col("match_id"),
    col("frame.timestamp").alias("timestamp_ms"),
    explode(col("frame.participantFrames")).alias("participant_key", "pf"),
    col("extraction_date"),
)

# Selecao dos campos finais
# minute = floor(timestamp_ms / 60000)
df_silver = df_pf.select(
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
    COMMENT 'Estado de cada jogador por minuto — Match-V5 timeline participantFrames'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
df_silver.createOrReplaceTempView("timeline_frames_updates")

# MERGE idempotente: upsert por (match_id, participant_id, timestamp_ms)
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.timeline_frames AS target
    USING timeline_frames_updates AS source
    ON target.match_id       = source.match_id
    AND target.participant_id = source.participant_id
    AND target.timestamp_ms   = source.timestamp_ms
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
for pk_col in ("match_id", "participant_id", "timestamp_ms"):
    null_count = df_final.filter(col(pk_col).isNull()).count()
    assert null_count == 0, f"FALHA: {null_count} linhas com {pk_col} nulo"

# Sem duplicatas na PK composta
total = df_final.count()
distinct = df_final.select("match_id", "participant_id", "timestamp_ms").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em timeline_frames (total={total}, distinct={distinct})"
)

# minute nunca negativo
negative_min = df_final.filter(col("minute") < 0).count()
assert negative_min == 0, f"FALHA: {negative_min} linhas com minute negativo"

# Timestamps dentro de intervalo razoavel (epoch ms entre 2020 e 2030)
ts_min_ms = df_final.selectExpr("min(timestamp_ms)").collect()[0][0]
ts_max_ms = df_final.selectExpr("max(timestamp_ms)").collect()[0][0]
# timestamp_ms aqui e relativo ao inicio da partida (milissegundos desde 00:00)
# portanto o valor minimo esperado eh >= 0 e max razoavel < 3 horas (10800000 ms)
assert ts_min_ms is not None and ts_min_ms >= 0, (
    f"FALHA: timestamp_ms minimo inesperado ({ts_min_ms})"
)

print(f"OK: loldata.cblol_silver.timeline_frames — {count} registros carregados")
print(f"    timestamp_ms: {ts_min_ms} -> {ts_max_ms}")
