import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DEFAULT_ARGS = {
    "owner": "data_engineers",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DATABRICKS_JOB_ID = int(os.environ["DATABRICKS_JOB_ID"])


@dag(
    dag_id="daily_lakehouse_ingest_dag",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["riot", "bronze", "autoloader", "databricks"],
)
def daily_lakehouse_ingest():
    """
    Daily Lakehouse Ingestion DAG:
    Triggers the Databricks Auto Loader Job (01_landing_to_bronze) to incrementally ingest
    partitioned S3 landing telemetry files into the flat bronze_matches Delta table.
    """
    DatabricksRunNowOperator(
        task_id="trigger_databricks_bronze_ingest",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
    )


daily_lakehouse_ingest_pipeline = daily_lakehouse_ingest()
