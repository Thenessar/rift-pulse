# Databricks notebook source
# MAGIC %md
# MAGIC # Landing to Bronze
# MAGIC
# MAGIC Incremental ingestion of immutable `raw_session_*.jsonl.gz` files from S3.
# MAGIC Auto Loader runs with `Trigger.AvailableNow`, writes a Delta table backed by
# MAGIC Parquet under `s3://<bucket>/bronze/match_snapshots`, and then terminates.

# COMMAND ----------
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def get_parameter(name: str, default: str) -> str:
    """Read a Databricks job parameter, then Spark config/env for local tests."""
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
bronze_table = require_identifier("bronze_table", get_parameter("bronze_table", "match_snapshots"))
environment = "prod"
orchestrator_run_id = get_parameter("orchestrator_run_id", "manual")

if not BUCKET_PATTERN.fullmatch(bucket):
    raise ValueError(f"Invalid S3 bucket name: {bucket!r}")
if not orchestrator_run_id or len(orchestrator_run_id) > 255:
    raise ValueError("orchestrator_run_id must contain between 1 and 255 characters")

landing_source = f"s3://{bucket}/landing/"
bronze_path = f"s3://{bucket}/bronze/match_snapshots"
state_root = f"s3://{bucket}/_state/{environment}/autoloader/match_snapshots/v1"
checkpoint_path = f"{state_root}/checkpoint"
schema_path = f"{state_root}/schema"
table_name = f"{catalog}.{bronze_schema}.{bronze_table}"

print(
    {
        "source": landing_source,
        "target_table": table_name,
        "target_path": bronze_path,
        "checkpoint": checkpoint_path,
        "schema_location": schema_path,
        "orchestrator_run_id": orchestrator_run_id,
    }
)

# COMMAND ----------
# The catalog and its S3 external location/storage credential must already exist.
# The job principal needs USE CATALOG plus CREATE SCHEMA/TABLE and storage access.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING DELTA
    LOCATION '{bronze_path}'
    """
)

# Select file metadata while it is still available from the file source. The legacy
# input_file_name() function is not available with Unity Catalog and DBR 17.3+.
df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "raw_session_*.jsonl.gz")
    .load(landing_source)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_size").alias("_source_file_size"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
    )
)

df_bronze = (
    df_raw.withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_orchestrator_run_id", F.lit(orchestrator_run_id))
    .withColumn(
        "_landing_date_from_path",
        F.regexp_extract(F.col("_source_file"), r"/date=(\d{4}-\d{2}-\d{2})/", 1),
    )
    .withColumn(
        "landing_date",
        F.expr("try_cast(nullif(_landing_date_from_path, '') AS DATE)"),
    )
    .withColumn(
        "_recording_id_from_filename",
        F.regexp_extract(F.col("_source_file_name"), r"^raw_session_([^.]+)\.jsonl\.gz$", 1),
    )
)

# COMMAND ----------
# Delta data files are Parquet files. The external table registered above keeps
# the bronze/ storage layout while the transaction log supplies atomicity.
query = (
    df_bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(table_name)
)

query.awaitTermination()
print(f"Landing to Bronze completed: {table_name}")
