# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver (MVP)
# MAGIC
# MAGIC Parses both the versioned landing envelope and legacy raw payloads. The MVP
# MAGIC rebuilds small Silver tables in full so late files and manifest corrections
# MAGIC are handled deterministically. It only models data needed by the balance
# MAGIC Gold layer: matches, observations, champion picks and final item builds.

# COMMAND ----------
import os
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def get_parameter(name: str, default: str) -> str:
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
silver_schema = require_identifier("silver_schema", get_parameter("silver_schema", "silver"))
observations_table = require_identifier(
    "bronze_observations_table",
    get_parameter("bronze_observations_table", "observations_raw"),
)
manifests_table = require_identifier(
    "bronze_manifests_table",
    get_parameter("bronze_manifests_table", "recording_manifests_raw"),
)

if not BUCKET_PATTERN.fullmatch(bucket):
    raise ValueError(f"Invalid S3 bucket name: {bucket!r}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{silver_schema}`")

# COMMAND ----------
item_schema = T.StructType(
    [
        T.StructField("itemID", T.LongType()),
        T.StructField("displayName", T.StringType()),
        T.StructField("count", T.LongType()),
        T.StructField("slot", T.LongType()),
        T.StructField("price", T.LongType()),
        T.StructField("consumable", T.BooleanType()),
    ]
)

player_schema = T.StructType(
    [
        T.StructField("riotId", T.StringType()),
        T.StructField("riotIdGameName", T.StringType()),
        T.StructField("riotIdTagLine", T.StringType()),
        T.StructField("summonerName", T.StringType()),
        T.StructField("championName", T.StringType()),
        T.StructField("team", T.StringType()),
        T.StructField("position", T.StringType()),
        T.StructField("isBot", T.BooleanType()),
        T.StructField("level", T.LongType()),
        T.StructField("items", T.ArrayType(item_schema)),
    ]
)

payload_schema = T.StructType(
    [
        T.StructField(
            "gameData",
            T.StructType(
                [
                    T.StructField("gameMode", T.StringType()),
                    T.StructField("gameTime", T.DoubleType()),
                    T.StructField("mapName", T.StringType()),
                    T.StructField("mapNumber", T.LongType()),
                    T.StructField("mapTerrain", T.StringType()),
                ]
            ),
        ),
        T.StructField("allPlayers", T.ArrayType(player_schema)),
    ]
)

envelope_schema = T.StructType(
    [
        T.StructField("schema_version", T.StringType()),
        T.StructField("recording_id", T.StringType()),
        T.StructField("observation_id", T.StringType()),
        T.StructField("sequence_no", T.LongType()),
        T.StructField("observed_at_utc", T.StringType()),
        T.StructField("collector_version", T.StringType()),
        T.StructField("payload_sha256", T.StringType()),
        T.StructField("payload_json", T.StringType()),
    ]
)

manifest_schema = T.StructType(
    [
        T.StructField("manifest_schema_version", T.StringType()),
        T.StructField("recording_id", T.StringType()),
        T.StructField("created_at_utc", T.StringType()),
        T.StructField("completion_status", T.StringType()),
        T.StructField("recorded_at_utc", T.StringType()),
        T.StructField("reported_total_ticks", T.LongType()),
        T.StructField("record_count_matches_summary", T.BooleanType()),
        T.StructField("observed_winner", T.StringType()),
        T.StructField("outcome_status", T.StringType()),
        T.StructField("outcome_source", T.StringType()),
        T.StructField("predicted_winner", T.StringType()),
        T.StructField(
            "raw",
            T.StructType(
                [
                    T.StructField("sha256", T.StringType()),
                    T.StructField("record_count", T.LongType()),
                    T.StructField("first_observed_at_utc", T.StringType()),
                    T.StructField("last_observed_at_utc", T.StringType()),
                ]
            ),
        ),
    ]
)

# COMMAND ----------
bronze_observations = spark.table(f"{catalog}.{bronze_schema}.{observations_table}")

parsed = (
    bronze_observations.withColumn("_envelope", F.from_json("_raw_json", envelope_schema))
    .withColumn(
        "schema_version",
        F.when(F.col("_envelope.payload_json").isNotNull(), F.col("_envelope.schema_version")).otherwise(
            F.lit("legacy")
        ),
    )
    .withColumn(
        "payload_json",
        F.when(F.col("_envelope.payload_json").isNotNull(), F.col("_envelope.payload_json")).otherwise(
            F.col("_raw_json")
        ),
    )
    .withColumn("_payload", F.from_json("payload_json", payload_schema))
    .withColumn(
        "recording_id",
        F.coalesce(F.col("_envelope.recording_id"), F.col("_recording_id_from_filename")),
    )
    .withColumn(
        "payload_sha256",
        F.coalesce(F.col("_envelope.payload_sha256"), F.sha2("payload_json", 256)),
    )
    .withColumn("observed_at_utc", F.to_timestamp(F.col("_envelope.observed_at_utc")))
    .withColumn("sequence_no", F.col("_envelope.sequence_no"))
    .withColumn(
        "observation_id",
        F.coalesce(
            F.col("_envelope.observation_id"),
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("recording_id"),
                    F.col("_payload.gameData.gameTime").cast("string"),
                    F.col("payload_sha256"),
                ),
                256,
            ),
        ),
    )
    .withColumn(
        "dq_errors",
        F.expr(
            "filter(array("
            "if(recording_id is null or recording_id = '', 'MISSING_RECORDING_ID', null),"
            "if(_payload is null, 'INVALID_PAYLOAD_JSON', null),"
            "if(_payload.gameData is null, 'MISSING_GAME_DATA', null),"
            "if(_payload.gameData.gameTime is null, 'MISSING_GAME_TIME', null),"
            "if(_payload.gameData.gameTime < 0, 'NEGATIVE_GAME_TIME', null),"
            "if(schema_version not in ('1.0', 'legacy'), 'UNSUPPORTED_SCHEMA_VERSION', null)"
            "), x -> x is not null)"
        ),
    )
)

identity_conflicts = (
    parsed.where(F.col("observation_id").isNotNull())
    .groupBy("observation_id")
    .agg(F.countDistinct("payload_sha256").alias("payload_versions"))
    .where(F.col("payload_versions") > 1)
    .select("observation_id")
    .withColumn("has_identity_conflict", F.lit(True))
)

parsed = (
    parsed.join(identity_conflicts, "observation_id", "left")
    .fillna(False, subset=["has_identity_conflict"])
    .withColumn(
        "dq_errors",
        F.when(
            F.col("has_identity_conflict"),
            F.concat(F.col("dq_errors"), F.array(F.lit("OBSERVATION_ID_CONFLICT"))),
        ).otherwise(F.col("dq_errors")),
    )
)

quarantine = parsed.where(F.size("dq_errors") > 0).select(
    "recording_id",
    "observation_id",
    "schema_version",
    "dq_errors",
    "_raw_json",
    "_source_file",
    "_ingested_at",
    "_orchestrator_run_id",
)

valid_rank = Window.partitionBy("observation_id").orderBy(F.col("_ingested_at").asc(), F.col("_source_file").asc())
valid = (
    parsed.where(F.size("dq_errors") == 0)
    .withColumn("_dedupe_rank", F.row_number().over(valid_rank))
    .where(F.col("_dedupe_rank") == 1)
    .drop("_dedupe_rank")
)

observations = valid.select(
    "observation_id",
    "recording_id",
    "sequence_no",
    "observed_at_utc",
    "schema_version",
    F.col("_envelope.collector_version").alias("collector_version"),
    "payload_sha256",
    F.col("_payload.gameData.gameTime").alias("game_time_seconds"),
    F.upper(F.col("_payload.gameData.gameMode")).alias("game_mode"),
    F.col("_payload.gameData.mapNumber").alias("map_id"),
    F.col("_payload.gameData.mapName").alias("map_name"),
    F.col("_payload.gameData.mapTerrain").alias("map_terrain"),
    F.size(F.col("_payload.allPlayers")).alias("participant_count"),
    "landing_date",
    "_source_file",
    "_ingested_at",
    "_orchestrator_run_id",
)

# COMMAND ----------
exploded_players = valid.select(
    "observation_id",
    "recording_id",
    "observed_at_utc",
    F.col("_payload.gameData.gameTime").alias("game_time_seconds"),
    F.posexplode(F.col("_payload.allPlayers")).alias("participant_slot", "player"),
).withColumn(
    "participant_key",
    F.sha2(F.concat_ws("||", F.col("recording_id"), F.col("participant_slot").cast("string")), 256),
)

last_player_window = Window.partitionBy("recording_id", "participant_slot").orderBy(
    F.col("game_time_seconds").desc(),
    F.col("observed_at_utc").desc_nulls_last(),
    F.col("observation_id").desc(),
)

final_players = (
    exploded_players.withColumn("_latest_rank", F.row_number().over(last_player_window))
    .where(F.col("_latest_rank") == 1)
    .drop("_latest_rank")
)

match_participants = final_players.select(
    "participant_key",
    "recording_id",
    "participant_slot",
    F.upper(F.col("player.team")).alias("team_source"),
    F.when(F.upper(F.col("player.team")) == "ORDER", F.lit("BLUE"))
    .when(F.upper(F.col("player.team")) == "CHAOS", F.lit("RED"))
    .alias("team"),
    F.col("player.championName").alias("champion_name"),
    F.upper(F.col("player.position")).alias("position_observed"),
    F.col("player.isBot").alias("is_bot"),
    F.coalesce(F.col("player.riotId"), F.col("player.summonerName")).isNotNull().alias("has_player_identity"),
    "observation_id",
    "game_time_seconds",
)

match_participant_items = (
    final_players.select(
        "participant_key",
        "recording_id",
        "participant_slot",
        "observation_id",
        "game_time_seconds",
        F.posexplode(F.col("player.items")).alias("item_array_position", "item"),
    )
    .select(
        "participant_key",
        "recording_id",
        "participant_slot",
        "observation_id",
        "game_time_seconds",
        F.coalesce(F.col("item.slot"), F.col("item_array_position").cast("long")).alias("item_slot"),
        F.col("item.itemID").alias("item_id"),
        F.col("item.displayName").alias("item_name"),
        F.coalesce(F.col("item.count"), F.lit(1)).alias("item_count"),
        F.col("item.price").alias("source_item_price"),
        F.col("item.consumable").alias("is_consumable"),
    )
    .where(F.col("item_id").isNotNull())
)

# COMMAND ----------
bronze_manifests = spark.table(f"{catalog}.{bronze_schema}.{manifests_table}")
manifests = (
    bronze_manifests.withColumn("manifest", F.from_json("_raw_json", manifest_schema))
    .where(F.col("manifest.recording_id").isNotNull())
    .select(
        F.col("manifest.recording_id").alias("recording_id"),
        F.col("manifest.completion_status").alias("completion_status"),
        F.col("manifest.raw.record_count").alias("manifest_record_count"),
        F.col("manifest.reported_total_ticks").alias("reported_total_ticks"),
        F.col("manifest.record_count_matches_summary").alias("record_count_matches_summary"),
        F.upper(F.col("manifest.observed_winner")).alias("observed_winner_source"),
        F.upper(F.col("manifest.outcome_status")).alias("outcome_status"),
        F.col("manifest.outcome_source").alias("outcome_source"),
        F.col("manifest.predicted_winner").alias("predicted_winner"),
        F.to_timestamp(F.col("manifest.recorded_at_utc")).alias("recorded_at_utc"),
        "_ingested_at",
        "_source_file",
    )
)

latest_manifest_window = Window.partitionBy("recording_id").orderBy(
    F.col("_ingested_at").desc(), F.col("_source_file").desc()
)
latest_manifests = (
    manifests.withColumn("_latest_rank", F.row_number().over(latest_manifest_window))
    .where(F.col("_latest_rank") == 1)
    .drop("_latest_rank", "_source_file")
)

observation_rollup = observations.groupBy("recording_id").agg(
    F.count("*").alias("observation_count"),
    F.min("game_time_seconds").alias("min_game_time_seconds"),
    F.max("game_time_seconds").alias("max_game_time_seconds"),
    F.min("observed_at_utc").alias("first_observed_at_utc"),
    F.max("observed_at_utc").alias("last_observed_at_utc"),
    F.first("game_mode", ignorenulls=True).alias("game_mode"),
    F.countDistinct("game_mode").alias("game_mode_versions"),
    F.first("map_id", ignorenulls=True).alias("map_id"),
    F.first("map_name", ignorenulls=True).alias("map_name"),
    F.min("participant_count").alias("min_participant_count"),
    F.max("participant_count").alias("max_participant_count"),
    F.min("landing_date").alias("landing_date"),
)

matches = (
    observation_rollup.join(latest_manifests, "recording_id", "left")
    .withColumn(
        "observed_winner",
        F.when(F.col("observed_winner_source") == "ORDER", F.lit("BLUE"))
        .when(F.col("observed_winner_source") == "CHAOS", F.lit("RED"))
        .when(F.col("observed_winner_source").isin("BLUE", "RED"), F.col("observed_winner_source")),
    )
    .withColumn(
        "is_outcome_confirmed",
        F.col("observed_winner").isNotNull() & F.col("outcome_status").isin("OBSERVED", "CONFIRMED"),
    )
    .withColumn(
        "record_count_matches_manifest",
        F.when(F.col("manifest_record_count").isNull(), F.lit(None).cast("boolean")).otherwise(
            F.col("observation_count") == F.col("manifest_record_count")
        ),
    )
    .withColumn(
        "is_complete",
        F.col("completion_status").startswith("COMPLETE") & (F.col("record_count_matches_manifest") == F.lit(True)),
    )
    .withColumn(
        "is_balance_eligible",
        F.col("is_complete")
        & (F.col("min_participant_count") == 10)
        & (F.col("max_participant_count") == 10)
        & (F.col("game_mode_versions") == 1)
        & (~F.col("game_mode").isin("PRACTICETOOL", "CHERRY", "ARENA")),
    )
    .drop("observed_winner_source", "_ingested_at")
)


# COMMAND ----------
def replace_silver_table(dataframe, table: str):
    """Full rebuild is deliberate for the MVP data volume."""
    table_name = f"{catalog}.{silver_schema}.{table}"
    table_path = f"s3://{bucket}/silver/{table}"
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_path)
        .saveAsTable(table_name)
    )


replace_silver_table(observations, "observations")
replace_silver_table(matches, "matches")
replace_silver_table(match_participants, "match_participants")
replace_silver_table(match_participant_items, "match_participant_items")
replace_silver_table(quarantine, "quarantine_observations")

print(
    {
        "silver_schema": f"{catalog}.{silver_schema}",
        "tables": [
            "observations",
            "matches",
            "match_participants",
            "match_participant_items",
            "quarantine_observations",
        ],
    }
)
