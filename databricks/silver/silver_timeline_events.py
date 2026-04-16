# Databricks notebook source
# Notebook : silver_timeline_events
# Origem   : loldata.cblol_bronze.timelines  (info.frames[].events[])
# Destino  : loldata.cblol_silver.timeline_events
# PK       : match_id + timestamp_ms + event_index
# Descricao: Explode o array info.frames[] e depois events[] de cada frame
#            para uma linha por evento por partida. Usa posexplode para
#            obter o indice do evento dentro do frame (event_index), garantindo
#            unicidade da PK mesmo com multiplos eventos no mesmo timestamp_ms.
#            Calcula minute = floor(timestamp_ms / 60000).

# COMMAND ----------

from pyspark.sql.functions import col, explode, floor, from_json, posexplode
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# Schema da posicao no mapa (presente em alguns tipos de evento)
schema_position = StructType(
    [
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
    ]
)

# Schema de um evento dentro de frames[].events[]
# Campos opcionais variam por tipo: CHAMPION_KILL, ITEM_PURCHASED, WARD_PLACED,
# BUILDING_KILL, ELITE_MONSTER_KILL, etc. Campos ausentes sao None no DataFrame.
schema_event = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("type", StringType(), True),
        StructField("killerId", IntegerType(), True),
        StructField("victimId", IntegerType(), True),
        StructField("assistingParticipantIds", ArrayType(IntegerType()), True),
        StructField("position", schema_position, True),
        StructField("itemId", IntegerType(), True),
        StructField("wardType", StringType(), True),
        StructField("buildingType", StringType(), True),
    ]
)

# Schema de um frame — apenas os campos necessarios para este notebook
# participantFrames e omitido (tratado em silver_timeline_frames)
schema_frame = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("events", ArrayType(schema_event), True),
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

# Explode 1: info.frames[] -> uma linha por frame por partida
df_frames = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.frames")).alias("frame"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# Explode 2: frame.events[] com posicao (posexplode)
# posexplode retorna (pos, col) -> pos = indice dentro do array (event_index)
df_events = df_frames.select(
    col("match_id"),
    col("frame.timestamp").alias("frame_timestamp_ms"),
    posexplode(col("frame.events")).alias("event_index", "event"),
    col("extraction_date"),
)

# Selecao dos campos finais
# timestamp_ms vem do proprio evento (pode diferir ligeiramente do frame timestamp)
# minute = floor(timestamp_ms / 60000)
df_silver = df_events.select(
    col("match_id"),
    col("event.timestamp").alias("timestamp_ms"),
    col("event_index"),
    floor(col("event.timestamp") / 60000).cast(IntegerType()).alias("minute"),
    col("event.type").alias("type"),
    col("event.killerId").alias("killer_id"),
    col("event.victimId").alias("victim_id"),
    col("event.assistingParticipantIds").alias("assisting_participant_ids"),
    col("event.position.x").alias("position_x"),
    col("event.position.y").alias("position_y"),
    col("event.itemId").alias("item_id"),
    col("event.wardType").alias("ward_type"),
    col("event.buildingType").alias("building_type"),
    col("extraction_date"),
).filter(col("timestamp_ms").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.timeline_events (
        match_id                        STRING,
        timestamp_ms                    LONG,
        event_index                     INT,
        minute                          INT,
        type                            STRING,
        killer_id                       INT,
        victim_id                       INT,
        assisting_participant_ids       ARRAY<INT>,
        position_x                      INT,
        position_y                      INT,
        item_id                         INT,
        ward_type                       STRING,
        building_type                   STRING,
        extraction_date                 DATE
    )
    USING DELTA
    COMMENT 'Eventos de timeline por partida — Match-V5 timeline frames[].events[]'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
df_silver.createOrReplaceTempView("timeline_events_updates")

# MERGE idempotente: upsert por (match_id, timestamp_ms, event_index)
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.timeline_events AS target
    USING timeline_events_updates AS source
    ON target.match_id     = source.match_id
    AND target.timestamp_ms = source.timestamp_ms
    AND target.event_index  = source.event_index
    WHEN MATCHED THEN
        UPDATE SET
            target.minute                       = source.minute,
            target.type                         = source.type,
            target.killer_id                    = source.killer_id,
            target.victim_id                    = source.victim_id,
            target.assisting_participant_ids    = source.assisting_participant_ids,
            target.position_x                   = source.position_x,
            target.position_y                   = source.position_y,
            target.item_id                      = source.item_id,
            target.ward_type                    = source.ward_type,
            target.building_type                = source.building_type,
            target.extraction_date              = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (
            match_id, timestamp_ms, event_index, minute, type,
            killer_id, victim_id, assisting_participant_ids,
            position_x, position_y, item_id, ward_type, building_type,
            extraction_date
        )
        VALUES (
            source.match_id, source.timestamp_ms, source.event_index,
            source.minute, source.type,
            source.killer_id, source.victim_id,
            source.assisting_participant_ids,
            source.position_x, source.position_y,
            source.item_id, source.ward_type, source.building_type,
            source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.timeline_events")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

# Chaves primarias nunca nulas
for pk_col in ("match_id", "timestamp_ms", "event_index"):
    null_count = df_final.filter(col(pk_col).isNull()).count()
    assert null_count == 0, f"FALHA: {null_count} linhas com {pk_col} nulo"

# Sem duplicatas na PK composta
total = df_final.count()
distinct = (
    df_final.select("match_id", "timestamp_ms", "event_index").distinct().count()
)
assert total == distinct, (
    f"FALHA: duplicatas em timeline_events (total={total}, distinct={distinct})"
)

# minute nunca negativo
negative_min = df_final.filter(col("minute") < 0).count()
assert negative_min == 0, f"FALHA: {negative_min} linhas com minute negativo"

# timestamp_ms (relativo ao inicio da partida) deve ser >= 0
ts_min_ms = df_final.selectExpr("min(timestamp_ms)").collect()[0][0]
assert ts_min_ms is not None and ts_min_ms >= 0, (
    f"FALHA: timestamp_ms minimo inesperado ({ts_min_ms})"
)

print(f"OK: loldata.cblol_silver.timeline_events — {count} registros carregados")
