# Databricks notebook source
# Notebook : silver_matches
# Origem   : loldata.cblol_bronze.matches
# Destino  : loldata.cblol_silver.matches
# PK       : match_id
# Descricao: Extrai campos raiz de cada partida (metadados, duracao, modo,
#            fila, plataforma e time vencedor). Timestamps convertidos de
#            epoch ms para TIMESTAMP.

# COMMAND ----------

from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# Schema do objetivo de cada time (usado dentro de teams[])
schema_objective = StructType(
    [
        StructField("first", BooleanType(), True),
        StructField("kills", IntegerType(), True),
    ]
)

# Schema do ban (usado dentro de teams[].bans[])
schema_ban = StructType(
    [
        StructField("championId", IntegerType(), True),
        StructField("pickTurn", IntegerType(), True),
    ]
)

# Schema dos objetivos de um time
schema_objectives = StructType(
    [
        StructField("baron", schema_objective, True),
        StructField("dragon", schema_objective, True),
        StructField("tower", schema_objective, True),
        StructField("inhibitor", schema_objective, True),
        StructField("riftHerald", schema_objective, True),
    ]
)

# Schema de cada time dentro de info.teams[]
schema_team = StructType(
    [
        StructField("teamId", IntegerType(), True),
        StructField("win", BooleanType(), True),
        StructField("objectives", schema_objectives, True),
        StructField("bans", ArrayType(schema_ban), True),
    ]
)

# Schema minimo de participante (apenas para satisfazer from_json no nivel raiz)
# O notebook silver_match_participants expande esses campos com detalhe completo
schema_participant_stub = StructType(
    [
        StructField("puuid", StringType(), True),
    ]
)

# Schema do campo metadata
schema_metadata = StructType(
    [
        StructField("dataVersion", StringType(), True),
        StructField("matchId", StringType(), True),
        StructField("participants", ArrayType(StringType()), True),
    ]
)

# Schema do campo info (somente campos necessarios para esta tabela)
schema_info = StructType(
    [
        StructField("gameCreation", LongType(), True),
        StructField("gameDuration", IntegerType(), True),
        StructField("gameEndTimestamp", LongType(), True),
        StructField("gameVersion", StringType(), True),
        StructField("gameMode", StringType(), True),
        StructField("gameType", StringType(), True),
        StructField("mapId", IntegerType(), True),
        StructField("queueId", IntegerType(), True),
        StructField("platformId", StringType(), True),
        StructField("teams", ArrayType(schema_team), True),
    ]
)

# Schema raiz do JSON de uma partida (Match-V5 detail)
schema_match = StructType(
    [
        StructField("metadata", schema_metadata, True),
        StructField("info", schema_info, True),
    ]
)

# COMMAND ----------

# Leitura da camada bronze
df_bronze = spark.read.table("loldata.cblol_bronze.matches")

# Parse do JSON com schema explicito
df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_match))

# Transformacao: campos raiz da partida
# game_creation e game_end_timestamp: epoch ms -> TIMESTAMP (dividir por 1000)
# winning_team_id: primeiro time do array com win = true
df_silver = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    (col("data.info.gameCreation").cast("double") / 1000).cast("timestamp").alias(
        "game_creation"
    ),
    col("data.info.gameDuration").alias("game_duration_seconds"),
    (col("data.info.gameEndTimestamp").cast("double") / 1000).cast("timestamp").alias(
        "game_end_timestamp"
    ),
    col("data.info.gameVersion").alias("game_version"),
    col("data.info.gameMode").alias("game_mode"),
    col("data.info.gameType").alias("game_type"),
    col("data.info.mapId").alias("map_id"),
    col("data.info.queueId").alias("queue_id"),
    col("data.info.platformId").alias("platform_id"),
    expr(
        "filter(data.info.teams, t -> t.win = true)[0].teamId"
    ).cast(IntegerType()).alias("winning_team_id"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.matches (
        match_id                STRING,
        game_creation           TIMESTAMP,
        game_duration_seconds   INT,
        game_end_timestamp      TIMESTAMP,
        game_version            STRING,
        game_mode               STRING,
        game_type               STRING,
        map_id                  INT,
        queue_id                INT,
        platform_id             STRING,
        winning_team_id         INT,
        extraction_date         DATE
    )
    USING DELTA
    COMMENT 'Metadados de cada partida CBLOL — Match-V5 detail (campos raiz)'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
df_silver.createOrReplaceTempView("matches_updates")

# MERGE idempotente: upsert por match_id
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.matches AS target
    USING matches_updates AS source
    ON target.match_id = source.match_id
    WHEN MATCHED THEN
        UPDATE SET
            target.game_creation          = source.game_creation,
            target.game_duration_seconds  = source.game_duration_seconds,
            target.game_end_timestamp     = source.game_end_timestamp,
            target.game_version           = source.game_version,
            target.game_mode              = source.game_mode,
            target.game_type              = source.game_type,
            target.map_id                 = source.map_id,
            target.queue_id               = source.queue_id,
            target.platform_id            = source.platform_id,
            target.winning_team_id        = source.winning_team_id,
            target.extraction_date        = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (
            match_id, game_creation, game_duration_seconds, game_end_timestamp,
            game_version, game_mode, game_type, map_id, queue_id,
            platform_id, winning_team_id, extraction_date
        )
        VALUES (
            source.match_id, source.game_creation, source.game_duration_seconds,
            source.game_end_timestamp, source.game_version, source.game_mode,
            source.game_type, source.map_id, source.queue_id,
            source.platform_id, source.winning_team_id, source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.matches")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(col("match_id").isNull()).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com match_id nulo"

total = df_final.count()
distinct = df_final.select("match_id").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em matches (total={total}, distinct={distinct})"
)

# Timestamps devem estar no intervalo 2020–hoje
ts_min = df_final.selectExpr("min(game_creation)").collect()[0][0]
ts_max = df_final.selectExpr("max(game_creation)").collect()[0][0]
assert ts_min is not None and str(ts_min) >= "2020-01-01", (
    f"FALHA: game_creation minima fora do intervalo esperado ({ts_min})"
)
assert ts_max is not None and str(ts_max) <= "2030-01-01", (
    f"FALHA: game_creation maxima fora do intervalo esperado ({ts_max})"
)

print(f"OK: loldata.cblol_silver.matches — {count} partidas carregadas")
print(f"    intervalo: {ts_min} -> {ts_max}")
