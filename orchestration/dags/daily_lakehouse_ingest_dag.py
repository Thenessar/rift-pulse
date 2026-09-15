from datetime import timedelta

import pendulum
from airflow.decorators import dag
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
    description="Trigger and monitor the Databricks Landing-to-Silver job.",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=WARSAW),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["rift-pulse", "bronze", "silver", "databricks"],
)
def daily_lakehouse_ingest():
    """Airflow orchestrates; all data processing remains in Databricks."""
    DatabricksRunNowOperator(
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


daily_lakehouse_ingest_pipeline = daily_lakehouse_ingest()
