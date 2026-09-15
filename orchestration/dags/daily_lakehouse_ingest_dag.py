from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

WARSAW = "Europe/Warsaw"

DEFAULT_ARGS = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="daily_lakehouse_ingest",
    description="Build Landing/Bronze/Silver in Databricks, then tested Gold with dbt.",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=WARSAW),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["rift-pulse", "bronze", "silver", "gold", "databricks", "dbt"],
)
def daily_lakehouse_ingest():
    """Run dbt only after the Databricks job has completed successfully."""
    build_silver = DatabricksRunNowOperator(
        task_id="run_databricks_lakehouse_ingest",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_job_id }}",
        job_parameters={
            "orchestrator_run_id": "{{ run_id }}",
            "orchestrator_logical_date": "{{ ts }}",
        },
        idempotency_token="rift-pulse-lakehouse-{{ ts_nodash }}",
        polling_period_seconds=15,
        databricks_retry_limit=3,
        databricks_retry_delay=5,
        wait_for_termination=True,
        deferrable=False,
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=90),
    )

    build_gold = BashOperator(
        task_id="build_gold_with_dbt",
        bash_command="""
set -Eeuo pipefail
/opt/dbt_venv/bin/dbt source freshness \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/.dbt \
  --target prod \
  --target-path /opt/airflow/dbt-target
/opt/dbt_venv/bin/dbt build \
  --fail-fast \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/.dbt \
  --target prod \
  --target-path /opt/airflow/dbt-target
""",
        execution_timeout=timedelta(minutes=60),
        append_env=True,
    )

    build_silver >> build_gold


daily_lakehouse_ingest_pipeline = daily_lakehouse_ingest()
