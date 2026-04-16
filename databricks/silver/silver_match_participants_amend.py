# Databricks notebook source
# Notebook : silver_match_participants_amend
# Pre-req  : silver_match_participants ja populada
# Origem   : loldata.cblol_bronze.matches  (info.participants[].participantId)
# Destino  : loldata.cblol_silver.match_participants (adiciona participant_id INT)
# Descricao: Adiciona a coluna participant_id (1-10) necessaria para o JOIN com
#            silver.timeline_frames na gold_player_frames_indexed.

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, row_number
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------

# Schema minimo: apenas puuid e participantId por participante
schema_participant = StructType(
    [
        StructField("puuid", StringType(), True),
        StructField("participantId", IntegerType(), True),
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

schema_match = StructType(
    [
        StructField("metadata", schema_metadata, True),
        StructField("info", schema_info, True),
    ]
)

# COMMAND ----------

df_bronze = spark.read.table("loldata.cblol_bronze.matches")

df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_match))

df_exploded = df_parsed.select(
    col("data.metadata.matchId").alias("match_id"),
    explode(col("data.info.participants")).alias("p"),
).filter(col("match_id").isNotNull())

df_source = df_exploded.select(
    col("match_id"),
    col("p.puuid").alias("puuid"),
    col("p.participantId").alias("participant_id"),
).filter(col("puuid").isNotNull()).filter(col("participant_id").isNotNull())

# Dedup: garante 1 participant_id por (match_id, puuid) caso bronze tenha duplicatas
w = Window.partitionBy("match_id", "puuid").orderBy(col("participant_id"))
df_deduped = (
    df_source
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

# Adiciona a coluna se ainda nao existir
spark.sql(
    "ALTER TABLE loldata.cblol_silver.match_participants ADD COLUMN IF NOT EXISTS participant_id INT"
)

# COMMAND ----------

df_deduped.createOrReplaceTempView("participant_id_updates")

# Apenas atualiza registros existentes — nao insere novas linhas
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.match_participants AS target
    USING participant_id_updates AS source
    ON target.match_id = source.match_id
    AND target.puuid = source.puuid
    WHEN MATCHED THEN
        UPDATE SET target.participant_id = source.participant_id
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.match_participants")

null_pid = df_final.filter(col("participant_id").isNull()).count()
assert null_pid == 0, f"FALHA: {null_pid} linhas com participant_id nulo"

invalid_pid = df_final.filter(
    (col("participant_id") < 1) | (col("participant_id") > 10)
).count()
assert invalid_pid == 0, (
    f"FALHA: {invalid_pid} linhas com participant_id fora de [1, 10]"
)

count = df_final.count()
print(f"OK: participant_id adicionado e populado em {count} registros")
