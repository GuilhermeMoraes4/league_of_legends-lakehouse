# Databricks notebook source
# Notebook : silver_match_participants
# Origem   : loldata.cblol_bronze.matches  (info.participants[])
# Destino  : loldata.cblol_silver.match_participants
# PK       : match_id + puuid
# Descricao: Explode o array info.participants[] para uma linha por jogador
#            por partida. Calcula KDA = (kills + assists) / greatest(deaths, 1).

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, greatest, lit, row_number
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------

# Schema de cada participante dentro de info.participants[]
schema_participant = StructType(
    [
        StructField("puuid", StringType(), True),
        StructField("teamId", IntegerType(), True),
        StructField("championName", StringType(), True),
        StructField("championId", IntegerType(), True),
        StructField("individualPosition", StringType(), True),
        StructField("role", StringType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("goldEarned", IntegerType(), True),
        StructField("totalMinionsKilled", IntegerType(), True),
        StructField("neutralMinionsKilled", IntegerType(), True),
        StructField("visionScore", IntegerType(), True),
        StructField("wardsPlaced", IntegerType(), True),
        StructField("wardsKilled", IntegerType(), True),
        StructField("totalDamageDealtToChampions", IntegerType(), True),
        StructField("totalDamageTaken", IntegerType(), True),
        StructField("totalHeal", IntegerType(), True),
        StructField("item0", IntegerType(), True),
        StructField("item1", IntegerType(), True),
        StructField("item2", IntegerType(), True),
        StructField("item3", IntegerType(), True),
        StructField("item4", IntegerType(), True),
        StructField("item5", IntegerType(), True),
        StructField("item6", IntegerType(), True),
        StructField("summoner1Id", IntegerType(), True),
        StructField("summoner2Id", IntegerType(), True),
        StructField("win", BooleanType(), True),
    ]
)

schema_metadata = StructType(
    [
        StructField("matchId", StringType(), True),
    ]
)

schema_info = StructType(
    [
        StructField("participants", ArrayType(schema_participant), True),
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

# Explode do array de participantes: 1 linha por jogador por partida
df_exploded = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.participants")).alias("p"),
    col("extraction_date"),
).filter(col("match_id").isNotNull())

# Selecao dos campos finais e calculo do KDA
# kda = (kills + assists) / greatest(deaths, 1) — garante divisor >= 1
df_silver = df_exploded.select(
    col("match_id"),
    col("p.puuid").alias("puuid"),
    col("p.teamId").alias("team_id"),
    col("p.championName").alias("champion_name"),
    col("p.championId").alias("champion_id"),
    col("p.individualPosition").alias("individual_position"),
    col("p.role").alias("role"),
    col("p.kills").alias("kills"),
    col("p.deaths").alias("deaths"),
    col("p.assists").alias("assists"),
    (
        (col("p.kills") + col("p.assists")).cast(DoubleType())
        / greatest(col("p.deaths"), lit(1)).cast(DoubleType())
    ).alias("kda"),
    col("p.goldEarned").alias("gold_earned"),
    col("p.totalMinionsKilled").alias("total_minions_killed"),
    col("p.neutralMinionsKilled").alias("neutral_minions_killed"),
    col("p.visionScore").alias("vision_score"),
    col("p.wardsPlaced").alias("wards_placed"),
    col("p.wardsKilled").alias("wards_killed"),
    col("p.totalDamageDealtToChampions").alias("total_damage_dealt_to_champions"),
    col("p.totalDamageTaken").alias("total_damage_taken"),
    col("p.totalHeal").alias("total_heal"),
    col("p.item0").alias("item0"),
    col("p.item1").alias("item1"),
    col("p.item2").alias("item2"),
    col("p.item3").alias("item3"),
    col("p.item4").alias("item4"),
    col("p.item5").alias("item5"),
    col("p.item6").alias("item6"),
    col("p.summoner1Id").alias("summoner1_id"),
    col("p.summoner2Id").alias("summoner2_id"),
    col("p.win").alias("win"),
    col("extraction_date"),
).filter(col("puuid").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.match_participants (
        match_id                            STRING,
        puuid                               STRING,
        team_id                             INT,
        champion_name                       STRING,
        champion_id                         INT,
        individual_position                 STRING,
        role                                STRING,
        kills                               INT,
        deaths                              INT,
        assists                             INT,
        kda                                 DOUBLE,
        gold_earned                         INT,
        total_minions_killed                INT,
        neutral_minions_killed              INT,
        vision_score                        INT,
        wards_placed                        INT,
        wards_killed                        INT,
        total_damage_dealt_to_champions     INT,
        total_damage_taken                  INT,
        total_heal                          INT,
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
        extraction_date                     DATE
    )
    USING DELTA
    COMMENT 'Um registro por jogador por partida — stats individuais (Match-V5)'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
# Deduplicacao: manter apenas a extracao mais recente por PK
window_dedup = Window.partitionBy("match_id", "puuid").orderBy(col("extraction_date").desc())
df_silver = df_silver.withColumn("rn", row_number().over(window_dedup)).filter(col("rn") == 1).drop("rn")

df_silver.createOrReplaceTempView("match_participants_updates")

# MERGE idempotente: upsert por (match_id, puuid)
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.match_participants AS target
    USING match_participants_updates AS source
    ON target.match_id = source.match_id
    AND target.puuid = source.puuid
    WHEN MATCHED THEN
        UPDATE SET
            target.team_id                          = source.team_id,
            target.champion_name                    = source.champion_name,
            target.champion_id                      = source.champion_id,
            target.individual_position              = source.individual_position,
            target.role                             = source.role,
            target.kills                            = source.kills,
            target.deaths                           = source.deaths,
            target.assists                          = source.assists,
            target.kda                              = source.kda,
            target.gold_earned                      = source.gold_earned,
            target.total_minions_killed             = source.total_minions_killed,
            target.neutral_minions_killed           = source.neutral_minions_killed,
            target.vision_score                     = source.vision_score,
            target.wards_placed                     = source.wards_placed,
            target.wards_killed                     = source.wards_killed,
            target.total_damage_dealt_to_champions  = source.total_damage_dealt_to_champions,
            target.total_damage_taken               = source.total_damage_taken,
            target.total_heal                       = source.total_heal,
            target.item0                            = source.item0,
            target.item1                            = source.item1,
            target.item2                            = source.item2,
            target.item3                            = source.item3,
            target.item4                            = source.item4,
            target.item5                            = source.item5,
            target.item6                            = source.item6,
            target.summoner1_id                     = source.summoner1_id,
            target.summoner2_id                     = source.summoner2_id,
            target.win                              = source.win,
            target.extraction_date                  = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (
            match_id, puuid, team_id, champion_name, champion_id,
            individual_position, role, kills, deaths, assists, kda,
            gold_earned, total_minions_killed, neutral_minions_killed,
            vision_score, wards_placed, wards_killed,
            total_damage_dealt_to_champions, total_damage_taken, total_heal,
            item0, item1, item2, item3, item4, item5, item6,
            summoner1_id, summoner2_id, win, extraction_date
        )
        VALUES (
            source.match_id, source.puuid, source.team_id,
            source.champion_name, source.champion_id,
            source.individual_position, source.role,
            source.kills, source.deaths, source.assists, source.kda,
            source.gold_earned, source.total_minions_killed,
            source.neutral_minions_killed, source.vision_score,
            source.wards_placed, source.wards_killed,
            source.total_damage_dealt_to_champions,
            source.total_damage_taken, source.total_heal,
            source.item0, source.item1, source.item2, source.item3,
            source.item4, source.item5, source.item6,
            source.summoner1_id, source.summoner2_id,
            source.win, source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.match_participants")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

# Chaves primarias nunca nulas
null_match_id = df_final.filter(col("match_id").isNull()).count()
assert null_match_id == 0, f"FALHA: {null_match_id} linhas com match_id nulo"

null_puuid = df_final.filter(col("puuid").isNull()).count()
assert null_puuid == 0, f"FALHA: {null_puuid} linhas com puuid nulo"

# Sem duplicatas na PK composta (match_id, puuid)
total = df_final.count()
distinct = df_final.select("match_id", "puuid").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em match_participants (total={total}, distinct={distinct})"
)

# kda nunca negativo
negative_kda = df_final.filter(col("kda") < 0).count()
assert negative_kda == 0, f"FALHA: {negative_kda} linhas com kda negativo"

# team_id deve ser 100 ou 200
invalid_teams = df_final.filter(~col("team_id").isin(100, 200)).count()
assert invalid_teams == 0, (
    f"FALHA: {invalid_teams} linhas com team_id fora de {{100, 200}}"
)

print(f"OK: loldata.cblol_silver.match_participants — {count} registros carregados")
