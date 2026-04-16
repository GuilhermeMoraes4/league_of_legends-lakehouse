"""
Executa as transformacoes gold no Databricks via SQL Statement Execution API.

Fluxo:
1. Health check do warehouse
2. CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold
3. Amend silver: ADD COLUMN participant_id + MERGE INTO para popular
4. CREATE TABLE + MERGE INTO para cada tabela gold
5. Validacoes de integridade referencial

Tabelas criadas/atualizadas:
  loldata.cblol_gold.gold_team_stats
  loldata.cblol_gold.gold_player_performance
  loldata.cblol_gold.gold_draft
  loldata.cblol_gold.gold_player_frames_indexed
"""

import logging

from src.upload.databricks_sql import DatabricksSQLClient

logger = logging.getLogger(__name__)


def upload_gold(host, token, warehouse_id):
    """Executa pipeline completo de carga da camada gold."""
    with DatabricksSQLClient(warehouse_id, host, token) as client:
        if not client.health_check():
            raise RuntimeError("SQL Warehouse inacessivel")
        _ensure_gold_schema(client)
        _amend_silver_participant_id(client)
        _load_gold_team_stats(client)
        _load_gold_player_performance(client)
        _load_gold_draft(client)
        _load_gold_player_frames_indexed(client)
        _validate_gold(client)


def _ensure_gold_schema(client):
    result = client.execute(
        "CREATE SCHEMA IF NOT EXISTS loldata.cblol_gold",
        wait_timeout="50s",
    )
    if result:
        logger.info("Schema loldata.cblol_gold verificado.")
    else:
        logger.error("Falha ao criar schema loldata.cblol_gold.")


def _amend_silver_participant_id(client):
    alter_result = client.execute(
        "ALTER TABLE loldata.cblol_silver.match_participants ADD COLUMNS (participant_id INT)",
        wait_timeout="50s",
    )
    if alter_result:
        logger.info("Coluna participant_id adicionada a silver.match_participants.")
    else:
        logger.info(
            "ALTER TABLE retornou erro (coluna ja existe ou outro motivo). Continuando."
        )

    merge_result = client.execute(
        """
        MERGE INTO loldata.cblol_silver.match_participants t
        USING (
            SELECT
                get_json_object(raw_json, '$.metadata.matchId') AS match_id,
                p.puuid,
                p.participantId AS participant_id
            FROM loldata.cblol_bronze.matches
            LATERAL VIEW explode(
                from_json(
                    get_json_object(raw_json, '$.info.participants'),
                    'ARRAY<STRUCT<participantId:INT, puuid:STRING>>'
                )
            ) AS p
        ) s
        ON t.match_id = s.match_id AND t.puuid = s.puuid
        WHEN MATCHED AND t.participant_id IS NULL
            THEN UPDATE SET t.participant_id = s.participant_id
        """,
        wait_timeout="120s",
    )
    if merge_result:
        logger.info("participant_id populado em silver.match_participants.")
    else:
        logger.error("Falha ao popular participant_id em silver.match_participants.")


def _load_gold_team_stats(client):
    create_result = client.execute(
        """
        CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_team_stats (
            match_id                STRING,
            team_id                 INT,
            win                     BOOLEAN,
            baron_kills             INT,
            dragon_kills            INT,
            tower_kills             INT,
            inhibitor_kills         INT,
            rift_herald_kills       INT,
            game_duration_seconds   INT,
            game_creation           TIMESTAMP,
            winning_team_id         INT,
            extraction_date         DATE
        )
        USING DELTA
        COMMENT 'Stats por time por partida — objetivos e resultado (sem remakes)'
        """,
        wait_timeout="50s",
    )
    if not create_result:
        logger.error("Falha ao criar gold_team_stats.")
        return

    merge_result = client.execute(
        """
        MERGE INTO loldata.cblol_gold.gold_team_stats t
        USING (
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER(
                        PARTITION BY match_id, team_id
                        ORDER BY extraction_date DESC
                    ) AS _rn
                FROM (
                    SELECT
                        t.match_id,
                        t.team_id,
                        t.win,
                        t.baron_kills,
                        t.dragon_kills,
                        t.tower_kills,
                        t.inhibitor_kills,
                        t.rift_herald_kills,
                        m.game_duration_seconds,
                        m.game_creation,
                        m.winning_team_id,
                        t.extraction_date
                    FROM loldata.cblol_silver.match_teams t
                    JOIN loldata.cblol_silver.matches m ON t.match_id = m.match_id
                    WHERE m.game_duration_seconds >= 300
                )
            ) WHERE _rn = 1
        ) s
        ON t.match_id = s.match_id AND t.team_id = s.team_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        wait_timeout="120s",
    )
    if merge_result:
        logger.info("gold_team_stats carregada com sucesso.")
    else:
        logger.error("Falha ao carregar gold_team_stats.")


def _load_gold_player_performance(client):
    create_result = client.execute(
        """
        CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_player_performance (
            match_id                            STRING,
            puuid                               STRING,
            game_name                           STRING,
            tag_line                            STRING,
            team                                STRING,
            team_id                             INT,
            champion_name                       STRING,
            champion_id                         INT,
            role                                STRING,
            individual_position                 STRING,
            kills                               INT,
            deaths                              INT,
            assists                             INT,
            kda                                 DOUBLE,
            gold_earned                         INT,
            total_minions_killed                INT,
            neutral_minions_killed              INT,
            total_cs                            INT,
            vision_score                        INT,
            wards_placed                        INT,
            wards_killed                        INT,
            total_damage_dealt_to_champions     INT,
            total_damage_taken                  INT,
            total_heal                          INT,
            gold_per_minute                     DOUBLE,
            damage_per_minute                   DOUBLE,
            cs_per_minute                       DOUBLE,
            damage_per_gold                     DOUBLE,
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
            game_duration_seconds               INT,
            extraction_date                     DATE
        )
        USING DELTA
        COMMENT 'Performance individual por jogador por partida — metricas calculadas (sem remakes)'
        """,
        wait_timeout="50s",
    )
    if not create_result:
        logger.error("Falha ao criar gold_player_performance.")
        return

    merge_result = client.execute(
        """
        MERGE INTO loldata.cblol_gold.gold_player_performance t
        USING (
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER(
                        PARTITION BY match_id, puuid
                        ORDER BY extraction_date DESC
                    ) AS _rn
                FROM (
                    SELECT
                        p.match_id,
                        p.puuid,
                        a.game_name,
                        a.tag_line,
                        a.team,
                        p.team_id,
                        p.champion_name,
                        p.champion_id,
                        p.role,
                        p.individual_position,
                        p.kills,
                        p.deaths,
                        p.assists,
                        p.kda,
                        p.gold_earned,
                        p.total_minions_killed,
                        p.neutral_minions_killed,
                        (p.total_minions_killed + p.neutral_minions_killed) AS total_cs,
                        p.vision_score,
                        p.wards_placed,
                        p.wards_killed,
                        p.total_damage_dealt_to_champions,
                        p.total_damage_taken,
                        p.total_heal,
                        ROUND(p.gold_earned / (m.game_duration_seconds / 60.0), 2)
                            AS gold_per_minute,
                        ROUND(p.total_damage_dealt_to_champions / (m.game_duration_seconds / 60.0), 2)
                            AS damage_per_minute,
                        ROUND((p.total_minions_killed + p.neutral_minions_killed) / (m.game_duration_seconds / 60.0), 2)
                            AS cs_per_minute,
                        ROUND(p.total_damage_dealt_to_champions / GREATEST(p.gold_earned, 1), 4)
                            AS damage_per_gold,
                        p.item0,
                        p.item1,
                        p.item2,
                        p.item3,
                        p.item4,
                        p.item5,
                        p.item6,
                        p.summoner1_id,
                        p.summoner2_id,
                        p.win,
                        m.game_duration_seconds,
                        p.extraction_date
                    FROM loldata.cblol_silver.match_participants p
                    JOIN loldata.cblol_silver.matches m ON p.match_id = m.match_id
                    JOIN loldata.cblol_silver.accounts a ON p.puuid = a.puuid
                    WHERE m.game_duration_seconds >= 300
                )
            ) WHERE _rn = 1
        ) s
        ON t.match_id = s.match_id AND t.puuid = s.puuid
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        wait_timeout="120s",
    )
    if merge_result:
        logger.info("gold_player_performance carregada com sucesso.")
    else:
        logger.error("Falha ao carregar gold_player_performance.")


def _load_gold_draft(client):
    create_result = client.execute(
        """
        CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_draft (
            match_id        STRING,
            team_id         INT,
            champion_id     INT,
            champion_name   STRING,
            type            STRING,
            draft_order     INT,
            extraction_date DATE
        )
        USING DELTA
        COMMENT 'Draft normalizado — picks e bans por time por partida (sem remakes)'
        """,
        wait_timeout="50s",
    )
    if not create_result:
        logger.error("Falha ao criar gold_draft.")
        return

    merge_result = client.execute(
        """
        MERGE INTO loldata.cblol_gold.gold_draft t
        USING (
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER(
                        PARTITION BY match_id, team_id, champion_id, type
                        ORDER BY extraction_date DESC
                    ) AS _rn
                FROM (
                    WITH champion_map AS (
                        SELECT DISTINCT champion_id, FIRST(champion_name) AS champion_name
                        FROM loldata.cblol_silver.match_participants
                        GROUP BY champion_id
                    ),
                    bans AS (
                        SELECT
                            t.match_id,
                            t.team_id,
                            CAST(ban_val AS INT) AS champion_id,
                            'ban' AS type,
                            (pos + 1) AS draft_order,
                            t.extraction_date
                        FROM loldata.cblol_silver.match_teams t
                        LATERAL VIEW posexplode(t.bans) AS pos, ban_val
                        JOIN loldata.cblol_silver.matches m ON t.match_id = m.match_id
                        WHERE CAST(ban_val AS INT) != -1
                          AND m.game_duration_seconds >= 300
                    ),
                    picks AS (
                        SELECT
                            p.match_id,
                            p.team_id,
                            p.champion_id,
                            'pick' AS type,
                            CAST(NULL AS INT) AS draft_order,
                            p.extraction_date
                        FROM loldata.cblol_silver.match_participants p
                        JOIN loldata.cblol_silver.matches m ON p.match_id = m.match_id
                        WHERE m.game_duration_seconds >= 300
                    ),
                    combined AS (
                        SELECT * FROM bans
                        UNION ALL
                        SELECT * FROM picks
                    )
                    SELECT
                        c.match_id,
                        c.team_id,
                        c.champion_id,
                        COALESCE(cm.champion_name, 'UNKNOWN') AS champion_name,
                        c.type,
                        c.draft_order,
                        c.extraction_date
                    FROM combined c
                    LEFT JOIN champion_map cm ON c.champion_id = cm.champion_id
                )
            ) WHERE _rn = 1
        ) s
        ON t.match_id = s.match_id
           AND t.team_id = s.team_id
           AND t.champion_id = s.champion_id
           AND t.type = s.type
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        wait_timeout="120s",
    )
    if merge_result:
        logger.info("gold_draft carregada com sucesso.")
    else:
        logger.error("Falha ao carregar gold_draft.")


def _load_gold_player_frames_indexed(client):
    create_result = client.execute(
        """
        CREATE TABLE IF NOT EXISTS loldata.cblol_gold.gold_player_frames_indexed (
            match_id                STRING,
            participant_id          INT,
            frame_index             INT,
            puuid                   STRING,
            champion_name           STRING,
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
        COMMENT 'Frames de timeline indexados por participante por partida (sem remakes)'
        """,
        wait_timeout="50s",
    )
    if not create_result:
        logger.error("Falha ao criar gold_player_frames_indexed.")
        return

    merge_result = client.execute(
        """
        MERGE INTO loldata.cblol_gold.gold_player_frames_indexed t
        USING (
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER(
                        PARTITION BY match_id, participant_id, frame_index
                        ORDER BY extraction_date DESC
                    ) AS _rn
                FROM (
                    SELECT
                        tf.match_id,
                        tf.participant_id,
                        ROW_NUMBER() OVER(
                            PARTITION BY tf.match_id, tf.participant_id
                            ORDER BY tf.timestamp_ms
                        ) AS frame_index,
                        mp.puuid,
                        mp.champion_name,
                        tf.timestamp_ms,
                        tf.minute,
                        tf.current_gold,
                        tf.total_gold,
                        tf.xp,
                        tf.level,
                        tf.minions_killed,
                        tf.jungle_minions_killed,
                        tf.position_x,
                        tf.position_y,
                        tf.extraction_date
                    FROM loldata.cblol_silver.timeline_frames tf
                    JOIN loldata.cblol_silver.match_participants mp
                        ON tf.match_id = mp.match_id
                        AND tf.participant_id = mp.participant_id
                    JOIN loldata.cblol_silver.matches m ON tf.match_id = m.match_id
                    WHERE m.game_duration_seconds >= 300
                )
            ) WHERE _rn = 1
        ) s
        ON t.match_id = s.match_id
           AND t.participant_id = s.participant_id
           AND t.frame_index = s.frame_index
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        wait_timeout="120s",
    )
    if merge_result:
        logger.info("gold_player_frames_indexed carregada com sucesso.")
    else:
        logger.error("Falha ao carregar gold_player_frames_indexed.")


def _validate_gold(client):
    checks = [
        (
            "gold_team_stats count",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_team_stats",
        ),
        (
            "gold_player_performance count",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_player_performance",
        ),
        (
            "gold_draft count",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_draft",
        ),
        (
            "gold_player_frames_indexed count",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_player_frames_indexed",
        ),
        (
            "matches com duration < 300 em gold_team_stats",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_team_stats WHERE game_duration_seconds < 300",
        ),
        (
            "matches com duration < 300 em gold_player_performance",
            "SELECT COUNT(*) FROM loldata.cblol_gold.gold_player_performance WHERE game_duration_seconds < 300",
        ),
        (
            "gold_team_stats partidas sem exatamente 2 linhas",
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, COUNT(*) AS n
                FROM loldata.cblol_gold.gold_team_stats
                GROUP BY match_id HAVING n != 2
            )
            """,
        ),
        (
            "gold_player_performance partidas sem exatamente 10 linhas",
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, COUNT(*) AS n
                FROM loldata.cblol_gold.gold_player_performance
                GROUP BY match_id HAVING n != 10
            )
            """,
        ),
        (
            "gold_draft partidas com count fora de [10, 20]",
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, COUNT(*) AS n
                FROM loldata.cblol_gold.gold_draft
                GROUP BY match_id HAVING n < 10 OR n > 20
            )
            """,
        ),
        (
            "gold_team_stats PKs duplicadas",
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, team_id, COUNT(*) AS n
                FROM loldata.cblol_gold.gold_team_stats
                GROUP BY match_id, team_id HAVING n > 1
            )
            """,
        ),
        (
            "gold_player_performance PKs duplicadas",
            """
            SELECT COUNT(*) FROM (
                SELECT match_id, puuid, COUNT(*) AS n
                FROM loldata.cblol_gold.gold_player_performance
                GROUP BY match_id, puuid HAVING n > 1
            )
            """,
        ),
    ]

    for label, sql in checks:
        result = client.execute(sql, wait_timeout="50s")
        if result is None:
            logger.warning("[validacao] ERRO ao executar: %s", label)
            continue

        rows = result.get("result", {}).get("data_array", [])
        value = int(rows[0][0]) if rows else -1

        if "count" in label and "sem" not in label and "fora" not in label and "duplicadas" not in label and "duration" not in label:
            if value == 0:
                logger.warning("[validacao] AVISO: %s = 0 (tabela vazia?)", label)
            else:
                logger.info("[validacao] OK: %s = %d", label, value)
        else:
            if value != 0:
                logger.warning("[validacao] AVISO: %s = %d (esperado: 0)", label, value)
            else:
                logger.info("[validacao] OK: %s = 0", label)

    logger.info("Validacao gold concluida.")
