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
    dag_id="daily_landing_to_bronze",
    description="Trigger and monitor the Databricks Landing-to-Bronze job.",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=WARSAW),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["rift-pulse", "bronze", "databricks"],
)
def daily_landing_to_bronze():
    """Airflow orchestrates; all data processing remains in Databricks."""
    DatabricksRunNowOperator(
        task_id="run_databricks_landing_to_bronze",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.databricks_job_id }}",
        job_parameters={
            "orchestrator_run_id": "{{ run_id }}",
            "orchestrator_logical_date": "{{ ts }}",
        },
        idempotency_token="rift-pulse-bronze-{{ ts_nodash }}",
        polling_period_seconds=15,
        databricks_retry_limit=3,
        databricks_retry_delay=5,
        wait_for_termination=True,
        deferrable=False,
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=90),
    )


daily_landing_to_bronze_pipeline = daily_landing_to_bronze()
