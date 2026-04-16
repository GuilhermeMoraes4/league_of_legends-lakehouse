# Databricks notebook source
# Notebook : silver_accounts
# Origem   : loldata.cblol_bronze.accounts
# Destino  : loldata.cblol_silver.accounts
# PK       : puuid
# Descricao: Normaliza contas dos jogadores CBLOL a partir do JSON cru
#            combinado de Account-V1 e Summoner-V4.

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
)

# COMMAND ----------

# Schema explicito do JSON cru (Account-V1 + Summoner-V4 mesclados)
schema_account = StructType(
    [
        StructField("puuid", StringType(), True),
        StructField("gameName", StringType(), True),
        StructField("tagLine", StringType(), True),
        StructField("summonerLevel", LongType(), True),
        StructField("profileIconId", IntegerType(), True),
    ]
)

# COMMAND ----------

# Leitura da camada bronze
df_bronze = spark.read.table("loldata.cblol_bronze.accounts")

# Parse do JSON com schema explicito
df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_account))

# Selecao e tipagem dos campos finais
df_silver = df_parsed.select(
    col("data.puuid").alias("puuid"),
    col("data.gameName").alias("game_name"),
    col("data.tagLine").alias("tag_line"),
    col("data.summonerLevel").cast(IntegerType()).alias("summoner_level"),
    col("data.profileIconId").alias("profile_icon_id"),
    col("extraction_date"),
).filter(col("puuid").isNotNull())

# COMMAND ----------

# Garante que o schema silver existe
spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_silver")

# Cria a tabela silver se ainda nao existir
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_silver.accounts (
        puuid            STRING,
        game_name        STRING,
        tag_line         STRING,
        summoner_level   INT,
        profile_icon_id  INT,
        extraction_date  DATE
    )
    USING DELTA
    COMMENT 'Contas dos jogadores CBLOL — Account-V1 + Summoner-V4'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
df_silver.createOrReplaceTempView("accounts_updates")

# MERGE idempotente: upsert por puuid
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.accounts AS target
    USING accounts_updates AS source
    ON target.puuid = source.puuid
    WHEN MATCHED THEN
        UPDATE SET
            target.game_name        = source.game_name,
            target.tag_line         = source.tag_line,
            target.summoner_level   = source.summoner_level,
            target.profile_icon_id  = source.profile_icon_id,
            target.extraction_date  = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (puuid, game_name, tag_line, summoner_level, profile_icon_id, extraction_date)
        VALUES (
            source.puuid,
            source.game_name,
            source.tag_line,
            source.summoner_level,
            source.profile_icon_id,
            source.extraction_date
        )
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_silver.accounts")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(col("puuid").isNull()).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com puuid nulo"

total = df_final.count()
distinct = df_final.select("puuid").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em accounts (total={total}, distinct={distinct})"
)

print(f"OK: loldata.cblol_silver.accounts — {count} jogadores carregados")
