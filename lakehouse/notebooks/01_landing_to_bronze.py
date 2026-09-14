# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01_landing_to_bronze: Auto Loader Ingest from S3 Landing to Flat Bronze Delta Table
# MAGIC
# MAGIC **Rift-Pulse Lakehouse Platform**
# MAGIC - Source: `s3://<bucket>/landing/date=YYYY-MM-DD/raw_session_*.jsonl.gz`
# MAGIC - Engine: Databricks Auto Loader (`cloudFiles`)
# MAGIC - Target: Flat Delta table `bronze_matches` (No physical `partitionBy`, optimized for native Data Skipping)
# MAGIC - Compaction: `OPTIMIZE bronze_matches` post-ingest

# COMMAND ----------
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession if not already provided (e.g. in standalone execution)
spark = SparkSession.builder.getOrCreate()

# Retrieve parameters (supports Databricks dbutils widgets, spark conf, and environment variables)
try:
    dbutils.widgets.text("bucket", "rift-pulse-data-lake", "S3 Bucket Name")
    dbutils.widgets.text("table_name", "bronze_matches", "Target Bronze Table")
    bucket = dbutils.widgets.get("bucket")
    table_name = dbutils.widgets.get("table_name")
except Exception:
    bucket = spark.conf.get("spark.riftpulse.bucket", os.environ.get("AWS_S3_BUCKET", "rift-pulse-data-lake"))
    table_name = spark.conf.get("spark.riftpulse.table_name", "bronze_matches")

landing_source = f"s3://{bucket}/landing/"
checkpoint_path = f"s3://{bucket}/_checkpoints/bronze_matches"
schema_path = f"s3://{bucket}/_checkpoints/bronze_matches_schema"

print(f"[01_landing_to_bronze] Source: {landing_source}")
print(f"[01_landing_to_bronze] Target Table: {table_name}")
print(f"[01_landing_to_bronze] Checkpoint: {checkpoint_path}")
print(f"[01_landing_to_bronze] Schema Location: {schema_path}")

# COMMAND ----------
# 1. Incremental ingestion with Auto Loader (cloudFiles)
# Auto Loader recursively discovers partitioned date=YYYY-MM-DD/ subdirectories.
df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(landing_source)
)

# COMMAND ----------
# 2. Add audit metadata and logical filtering columns (Data Skipping)
# The Bronze layer deliberately has no physical partitioning (no partitionBy)
# to avoid the small-files problem. The game_date and session_id columns improve
# Data Skipping through Delta transaction metadata (_delta_log).
df_bronze = (
    df_raw.withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn(
        "game_date",
        F.coalesce(
            F.to_date(F.col("gameCreation")),
            F.to_date(F.col("date")),
            F.to_date(F.regexp_extract(F.input_file_name(), r"date=(\d{4}-\d{2}-\d{2})", 1)),
            F.to_date(F.current_timestamp()),
        ),
    )
    .withColumn("session_id", F.regexp_extract(F.input_file_name(), r"raw_session_([a-f0-9\-]+)", 1))
)

# COMMAND ----------
# 3. Stream in batch mode (Trigger.AvailableNow) into a flat Delta table
query = (
    df_bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(table_name)
)

print("[01_landing_to_bronze] Awaiting incremental stream batch termination...")
query.awaitTermination()
print("[01_landing_to_bronze] Stream batch completed successfully.")

# COMMAND ----------
# 4. Compact small Parquet files into optimally sized blocks
print(f"[01_landing_to_bronze] Running OPTIMIZE on {table_name}...")
spark.sql(f"OPTIMIZE {table_name}")
print(f"[01_landing_to_bronze] OPTIMIZE {table_name} completed.")
