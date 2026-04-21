# Databricks notebook source
# Notebook : silver_accounts
# Origem   : loldata.cblol_bronze.accounts
# Destino  : loldata.cblol_silver.accounts
# PK       : puuid
# Descricao: Normaliza contas dos jogadores CBLOL a partir do JSON cru.
#            O raw_json eh um JSON ARRAY (um array com ~40 jogadores por extracao),
#            por isso usa ArrayType + explode para desnormalizar.

# COMMAND ----------

from pyspark.sql.functions import col, explode, from_json, row_number
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------

# Schema de cada elemento do array JSON
# O raw_json contem um array de objetos com campos ja em snake_case
schema_account_element = StructType(
    [
        StructField("puuid", StringType(), True),
        StructField("game_name", StringType(), True),
        StructField("tag_line", StringType(), True),
        StructField("team", StringType(), True),
        StructField("role", StringType(), True),
    ]
)

# Schema completo: array de accounts
schema_accounts_array = ArrayType(schema_account_element)

# COMMAND ----------

# Leitura da camada bronze
df_bronze = spark.read.table("loldata.cblol_bronze.accounts")

# Parse do JSON array com schema explicito
df_parsed = df_bronze.withColumn("data", from_json(col("raw_json"), schema_accounts_array))

# Explode do array: 1 linha por jogador por extracao
df_exploded = df_parsed.select(
    explode(col("data")).alias("account"),
    col("extraction_date"),
)

# Selecao dos campos finais
df_silver = df_exploded.select(
    col("account.puuid").alias("puuid"),
    col("account.game_name").alias("game_name"),
    col("account.tag_line").alias("tag_line"),
    col("account.team").alias("team"),
    col("account.role").alias("role"),
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
        team             STRING,
        role             STRING,
        extraction_date  DATE
    )
    USING DELTA
    COMMENT 'Contas dos jogadores CBLOL — 1 linha por jogador (puuid)'
    """
)

# COMMAND ----------

# View temporaria para o MERGE
# Deduplicacao: manter apenas a extracao mais recente por PK
window_dedup = Window.partitionBy("puuid").orderBy(col("extraction_date").desc())
df_silver = df_silver.withColumn("rn", row_number().over(window_dedup)).filter(col("rn") == 1).drop("rn")

df_silver.createOrReplaceTempView("accounts_updates")

# MERGE idempotente: upsert por puuid
# Atualiza todos os campos caso o jogador mude de time ou role entre extracoes
spark.sql(
    """
    MERGE INTO loldata.cblol_silver.accounts AS target
    USING accounts_updates AS source
    ON target.puuid = source.puuid
    WHEN MATCHED THEN
        UPDATE SET
            target.game_name        = source.game_name,
            target.tag_line         = source.tag_line,
            target.team             = source.team,
            target.role             = source.role,
            target.extraction_date  = source.extraction_date
    WHEN NOT MATCHED THEN
        INSERT (puuid, game_name, tag_line, team, role, extraction_date)
        VALUES (
            source.puuid,
            source.game_name,
            source.tag_line,
            source.team,
            source.role,
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
