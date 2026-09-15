# Databricks notebook source
# MAGIC %md
# MAGIC # Landing to Bronze
# MAGIC
# MAGIC Incrementally ingests immutable observation JSONL.GZ files and recording
# MAGIC manifests. Bronze stores the original JSON text plus file lineage; parsing,
# MAGIC validation and normalization belong to Silver.

# COMMAND ----------
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def get_parameter(name: str, default: str) -> str:
    """Read a Databricks job parameter, then Spark config/env for tests."""
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name).strip()
    except Exception:
        spark_key = f"spark.riftpulse.{name}"
        return str(spark.conf.get(spark_key, os.environ.get(name.upper(), default))).strip()


def require_identifier(name: str, value: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid {name}: {value!r}")
    return value


bucket = get_parameter("bucket", "rift-pulse-data-lake")
catalog = require_identifier("catalog", get_parameter("catalog", "rift_pulse"))
bronze_schema = require_identifier("bronze_schema", get_parameter("bronze_schema", "bronze"))
observations_table = require_identifier(
    "bronze_observations_table",
    get_parameter("bronze_observations_table", "observations_raw"),
)
manifests_table = require_identifier(
    "bronze_manifests_table",
    get_parameter("bronze_manifests_table", "recording_manifests_raw"),
)
environment = get_parameter("environment", "prod")
orchestrator_run_id = get_parameter("orchestrator_run_id", "manual")

if not BUCKET_PATTERN.fullmatch(bucket):
    raise ValueError(f"Invalid S3 bucket name: {bucket!r}")
if not orchestrator_run_id or len(orchestrator_run_id) > 255:
    raise ValueError("orchestrator_run_id must contain between 1 and 255 characters")

landing_source = f"s3://{bucket}/landing/"
state_root = f"s3://{bucket}/_state/{environment}/autoloader"


def ingest_text_dataset(dataset: str, path_glob: str, target_table: str, target_path: str):
    """Ingest one immutable text dataset with an independent checkpoint."""
    checkpoint_path = f"{state_root}/{dataset}/v1/checkpoint"
    table_name = f"{catalog}.{bronze_schema}.{target_table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
          _raw_json STRING,
          _source_file STRING,
          _source_file_name STRING,
          _source_file_size BIGINT,
          _source_file_modified_at TIMESTAMP,
          _ingested_at TIMESTAMP,
          _orchestrator_run_id STRING,
          landing_date DATE,
          _recording_id_from_filename STRING
        )
        USING DELTA
        LOCATION '{target_path}'
        """
    )

    source = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("pathGlobFilter", path_glob)
        .load(landing_source)
        .select(
            F.col("value").alias("_raw_json"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_size").alias("_source_file_size"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        )
    )

    prepared = (
        source.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_orchestrator_run_id", F.lit(orchestrator_run_id))
        .withColumn(
            "landing_date",
            F.to_date(F.regexp_extract(F.col("_source_file"), r"/date=(\d{4}-\d{2}-\d{2})/", 1)),
        )
        .withColumn(
            "_recording_id_from_filename",
            F.regexp_extract(
                F.col("_source_file_name"),
                r"(?:raw_session_|recording_)([0-9A-Za-z-]+?)(?:_[0-9a-f]{16})?(?:\.jsonl\.gz|_manifest(?:_[0-9a-f]{16})?\.json)$",
                1,
            ),
        )
    )

    query = (
        prepared.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    query.awaitTermination()


# COMMAND ----------
ingest_text_dataset(
    dataset="observations_raw",
    path_glob="raw_session_*.jsonl.gz",
    target_table=observations_table,
    target_path=f"s3://{bucket}/bronze/observations_raw",
)

ingest_text_dataset(
    dataset="recording_manifests_raw",
    path_glob="recording_*_manifest_*.json",
    target_table=manifests_table,
    target_path=f"s3://{bucket}/bronze/recording_manifests_raw",
)

print(
    {
        "source": landing_source,
        "observations_table": f"{catalog}.{bronze_schema}.{observations_table}",
        "manifests_table": f"{catalog}.{bronze_schema}.{manifests_table}",
        "orchestrator_run_id": orchestrator_run_id,
    }
)
