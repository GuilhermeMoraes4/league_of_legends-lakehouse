# Databricks notebook source
# Notebook : silver_match_teams
# Origem   : loldata.cblol_bronze.matches  (info.teams[])
# Destino  : loldata.cblol_silver.match_teams
# PK       : match_id + team_id
# Descricao: Explode o array info.teams[] para uma linha por time por partida.
#            Extrai objetivos (baron, dragon, tower, inhibitor, riftHerald)
#            e bans como ARRAY<STRING> de champion IDs.

# COMMAND ----------

from pyspark.sql.functions import col, expr, explode, from_json, transform
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

# Sub-schemas reutilizados
schema_objective = StructType(
    [
        StructField("first", BooleanType(), True),
        StructField("kills", IntegerType(), True),
    ]
)

schema_objectives = StructType(
    [
        StructField("baron", schema_objective, True),
        StructField("dragon", schema_objective, True),
        StructField("tower", schema_objective, True),
        StructField("inhibitor", schema_objective, True),
        StructField("riftHerald", schema_objective, True),
    ]
)

schema_ban = StructType(
    [
        StructField("championId", IntegerType(), True),
        StructField("pickTurn", IntegerType(), True),
    ]
)

schema_team = StructType(
    [
        StructField("teamId", IntegerType(), True),
        StructField("win", BooleanType(), True),
        StructField("objectives", schema_objectives, True),
        StructField("bans", ArrayType(schema_ban), True),
    ]
)

# Schema minimo de participante (ignorado neste notebook)
schema_participant_stub = StructType(
    [
        StructField("puuid", StringType(), True),
    ]
)

schema_metadata = StructType(
    [
        StructField("matchId", StringType(), True),
    ]
)

schema_info = StructType(
    [
        StructField("teams", ArrayType(schema_team), True),
    ]
)

# Schema raiz do JSON de uma partida
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

# Explode do array de times: 1 linha por time por partida
df_exploded = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.teams")).alias("team"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# Selecao dos campos finais
# bans: converte array de structs {championId, pickTurn} para ARRAY<STRING>
df_silver = df_exploded.select(
    col("match_id"),
    col("team.teamId").alias("team_id"),
    col("team.win").alias("win"),
    col("team.objectives.baron.kills").alias("baron_kills"),
    col("team.objectives.dragon.kills").alias("dragon_kills"),
    col("team.objectives.tower.kills").alias("tower_kills"),
    col("team.objectives.inhibitor.kills").alias("inhibitor_kills"),
    col("team.objectives.riftHerald.kills").alias("rift_herald_kills"),
    expr(
        "transform(team.bans, b -> CAST(b.championId AS STRING))"
    ).alias("bans"),
    col("extraction_date"),
).filter(col("team_id").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.match_teams (
        match_id            STRING,
        team_id             INT,
        win                 BOOLEAN,
        baron_kills         INT,
        dragon_kills        INT,
        tower_kills         INT,
        inhibitor_kills     INT,
        rift_herald_kills   INT,
        bans                ARRAY<STRING>,
        extraction_date     DATE
    )
    USING DELTA
    COMMENT 'Um registro por time por partida — objetivos e bans (Match-V5)'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
df_silver.createOrReplaceTempView("match_teams_updates")

# MERGE idempotente: upsert por (match_id, team_id)
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.match_teams AS target
    USING match_teams_updates AS source
    ON target.match_id = source.match_id
    AND target.team_id = source.team_id
    WHEN MATCHED THEN
        UPDATE SET
            target.win                = source.win,
            target.baron_kills        = source.baron_kills,
            target.dragon_kills       = source.dragon_kills,
            target.tower_kills        = source.tower_kills,
            target.inhibitor_kills    = source.inhibitor_kills,
            target.rift_herald_kills  = source.rift_herald_kills,
            target.bans               = source.bans,
            target.extraction_date    = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (
            match_id, team_id, win, baron_kills, dragon_kills,
            tower_kills, inhibitor_kills, rift_herald_kills,
            bans, extraction_date
        )
        VALUES (
            source.match_id, source.team_id, source.win,
            source.baron_kills, source.dragon_kills,
            source.tower_kills, source.inhibitor_kills,
            source.rift_herald_kills, source.bans, source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.match_teams")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

# Chaves primarias nunca nulas
null_match_id = df_final.filter(col("match_id").isNull()).count()
assert null_match_id == 0, f"FALHA: {null_match_id} linhas com match_id nulo"

null_team_id = df_final.filter(col("team_id").isNull()).count()
assert null_team_id == 0, f"FALHA: {null_team_id} linhas com team_id nulo"

# Sem duplicatas na PK composta (match_id, team_id)
total = df_final.count()
distinct = df_final.select("match_id", "team_id").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em match_teams (total={total}, distinct={distinct})"
)

# team_id deve ser 100 ou 200 (unico padrao da Riot API)
invalid_teams = df_final.filter(~col("team_id").isin(100, 200)).count()
assert invalid_teams == 0, (
    f"FALHA: {invalid_teams} linhas com team_id fora de {{100, 200}}"
)

print(f"OK: loldata.cblol_silver.match_teams — {count} registros carregados")
