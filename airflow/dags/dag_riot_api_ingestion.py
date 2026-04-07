"""
DAG de ingestao da Riot Games API usando TaskFlow API.

Flow: extract_accounts >> extract_match_ids >> extract_match_details >> extract_timelines >> load_to_databricks

Cada task importa a funcao Python diretamente (sem BashOperator).
"""

import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task


def _ensure_pythonpath():
    """Garante que /opt/airflow esta no sys.path (Airflow 3.x SDK task runner)."""
    if "/opt/airflow" not in sys.path:
        sys.path.insert(0, "/opt/airflow")


def _get_ds(**kwargs):
    """Extrai execution date como string YYYY-MM-DD, compativel com Airflow 3.x."""
    if "ds" in kwargs:
        return kwargs["ds"]
    logical_date = kwargs.get("logical_date")
    if logical_date:
        return logical_date.strftime("%Y-%m-%d")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


default_args = {
    "owner": "guilherme_moraes",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="riot_api_ingestion",
    default_args=default_args,
    description="Extracao da Riot Games API e carregamento em Delta tables no Databricks",
    schedule="@daily",
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=["league_of_legends", "ingestion", "bronze"],
)
def riot_api_ingestion():

    @task
    def extract_accounts(**kwargs):
        _ensure_pythonpath()
        from src.extract.main import step_accounts
        step_accounts(_get_ds(**kwargs))

    @task
    def extract_match_ids(**kwargs):
        _ensure_pythonpath()
        from src.extract.main import step_match_ids
        step_match_ids(_get_ds(**kwargs))

    @task
    def extract_match_details(**kwargs):
        _ensure_pythonpath()
        from src.extract.main import step_match_details
        step_match_details(_get_ds(**kwargs))

    @task
    def extract_timelines(**kwargs):
        _ensure_pythonpath()
        from src.extract.main import step_timelines
        step_timelines(_get_ds(**kwargs))

    @task
    def load_to_databricks(**kwargs):
        _ensure_pythonpath()
        from src.upload.upload_bronze import upload_bronze
        upload_bronze(_get_ds(**kwargs))

    extract_accounts() >> extract_match_ids() >> extract_match_details() >> extract_timelines() >> load_to_databricks()


riot_api_ingestion()
