# Databricks notebook source
# Notebook : gold_draft
# Origem   : loldata.cblol_silver.match_teams (bans) + match_participants (picks)
# Destino  : loldata.cblol_gold.gold_draft
# PK       : match_id + team_id + champion_id + type
# Descricao: Draft normalizado — 1 linha por champion por type (pick/ban) por time
#            por partida. Bans com champion_id sem mapa conhecido = 'UNKNOWN'.
#            Exclui remakes (game_duration_seconds < 300).

# COMMAND ----------

from pyspark.sql.functions import col, lit, posexplode, row_number, when
from pyspark.sql.window import Window

# COMMAND ----------

df_teams = spark.read.table("loldata.cblol_silver.match_teams")
df_participants = spark.read.table("loldata.cblol_silver.match_participants")
df_matches = spark.read.table("loldata.cblol_silver.matches")

df_valid_matches = df_matches.filter(col("game_duration_seconds") >= 300).select("match_id")

# Mapa champion_id -> champion_name a partir de todos os picks conhecidos
df_champion_map = df_participants.select("champion_id", "champion_name").distinct()

# COMMAND ----------

# Bans: posexplode do ARRAY<STRING>, cast para INT, filtrar bans vazios (-1)
df_bans_raw = df_teams.select(
    col("match_id"),
    col("team_id"),
    posexplode(col("bans")).alias("pos", "ban_id"),
    col("extraction_date"),
).select(
    col("match_id"),
    col("team_id"),
    col("ban_id").cast("int").alias("champion_id"),
    lit("ban").alias("type"),
    (col("pos") + 1).alias("draft_order"),
    col("extraction_date"),
).filter(col("champion_id") != -1)

# Resolve champion_name para bans via left join; sem match = 'UNKNOWN'
df_bans = df_bans_raw.join(df_champion_map, on="champion_id", how="left").withColumn(
    "champion_name",
    when(col("champion_name").isNull(), lit("UNKNOWN")).otherwise(col("champion_name")),
)

# Picks: 1 linha por champion por time, draft_order nulo
df_picks = df_participants.select(
    col("match_id"),
    col("team_id"),
    col("champion_id"),
    col("champion_name"),
    lit("pick").alias("type"),
    lit(None).cast("int").alias("draft_order"),
    col("extraction_date"),
)

# COMMAND ----------

# Union bans + picks; aplica filtro de remakes
df_draft = (
    df_bans.select(
        "match_id", "team_id", "champion_id", "champion_name",
        "type", "draft_order", "extraction_date"
    )
    .unionAll(
        df_picks.select(
            "match_id", "team_id", "champion_id", "champion_name",
            "type", "draft_order", "extraction_date"
        )
    )
    .join(df_valid_matches, on="match_id")
)

# COMMAND ----------

# Dedup por PK antes do MERGE
w = Window.partitionBy("match_id", "team_id", "champion_id", "type").orderBy(
    col("extraction_date").desc()
)
df_deduped = (
    df_draft
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .drop("extraction_date")
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_draft (
        match_id        STRING,
        team_id         INT,
        champion_id     INT,
        champion_name   STRING,
        type            STRING,
        draft_order     INT
    )
    USING DELTA
    COMMENT 'Draft normalizado — picks e bans por time por partida (sem remakes)'
    """
)

# COMMAND ----------

df_deduped.createOrReplaceTempView("source")

spark.sql(
    """
    MERGE INTO loldata.cblol_gold.gold_draft t
    USING source s
    ON t.match_id = s.match_id
    AND t.team_id = s.team_id
    AND t.champion_id = s.champion_id
    AND t.type = s.type
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

# COMMAND ----------

# Assertions inline — validacao pos-escrita
df_final = spark.read.table("loldata.cblol_gold.gold_draft")

count = df_final.count()
assert count > 0, f"FALHA: tabela vazia (count={count})"

null_pk = df_final.filter(
    col("match_id").isNull()
    | col("team_id").isNull()
    | col("champion_id").isNull()
    | col("type").isNull()
).count()
assert null_pk == 0, f"FALHA: {null_pk} linhas com PK nula"

total = df_final.count()
distinct = df_final.select("match_id", "team_id", "champion_id", "type").distinct().count()
assert total == distinct, (
    f"FALHA: duplicatas em gold_draft (total={total}, distinct={distinct})"
)

invalid_type = df_final.filter(~col("type").isin("pick", "ban")).count()
assert invalid_type == 0, f"FALHA: {invalid_type} linhas com type invalido (esperado: pick|ban)"

invalid_champion = df_final.filter(col("champion_id") == -1).count()
assert invalid_champion == 0, f"FALHA: {invalid_champion} linhas com champion_id = -1"

bad_draft_counts = spark.sql(
    """
    SELECT COUNT(*) FROM (
        SELECT match_id, COUNT(*) AS n
        FROM loldata.cblol_gold.gold_draft
        GROUP BY match_id HAVING n < 10 OR n > 20
    )
    """
).collect()[0][0]
assert bad_draft_counts == 0, (
    f"FALHA: {bad_draft_counts} partidas com count fora de [10, 20] em gold_draft"
)

print(f"OK: loldata.cblol_gold.gold_draft — {count} registros carregados")
