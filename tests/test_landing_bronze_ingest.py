import ast
import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from services.archiver.s3_archiver import S3Archiver


class TestS3LandingUpload(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = self.temp_dir.name
        self.archiver = S3Archiver(data_dir=self.data_dir)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_upload_session_with_explicit_game_end_date(self):
        """Verifies that an explicit game_end_date creates the landing partition landing/date=YYYY-MM-DD/."""
        session_id = "test-session-explicit-date"
        raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps({"gameData": {"gameTime": 100}}) + "\n" * 10)

        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            result = self.archiver.upload_session_to_landing(session_id=session_id, game_end_date="2026-09-15")

            self.assertEqual(result["status"], "success")
            self.assertEqual(mock_s3.upload_file.call_count, 2)
            raw_key, manifest_key = result["uploaded_keys"]
            self.assertTrue(raw_key.startswith(f"landing/date=2026-09-15/raw_session_{session_id}_"))
            self.assertTrue(raw_key.endswith(".jsonl.gz"))
            self.assertTrue(manifest_key.startswith(f"landing/date=2026-09-15/recording_{session_id}_manifest_"))
            self.assertTrue(manifest_key.endswith(".json"))

            manifest_path = os.path.join(self.data_dir, f"recording_{session_id}_manifest.json")
            with open(manifest_path, encoding="utf-8") as f:
                manifest = json.load(f)
            self.assertEqual(manifest["raw"]["record_count"], 1)
            self.assertEqual(manifest["raw"]["schema_versions"], ["legacy"])
            self.assertEqual(manifest["completion_status"], "INCOMPLETE")

    def test_upload_session_derived_from_summary_recorded_at(self):
        """Verifies that partition date is deterministically derived from summary recorded_at (UTC)."""
        session_id = "test-session-summary-date"
        raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps({"gameData": {"gameTime": 200}}) + "\n" * 10)

        summary_path = os.path.join(self.data_dir, f"session_{session_id}_summary.json")
        summary_content = {
            "session_id": session_id,
            "recorded_at": "2026-09-14T23:59:58.123456Z",
            "duration_formatted": "35:12",
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary_content, f)

        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            result = self.archiver.upload_session_to_landing(session_id=session_id)

            self.assertEqual(result["status"], "success")
            raw_key, summary_key, manifest_key = result["uploaded_keys"]
            self.assertTrue(raw_key.startswith(f"landing/date=2026-09-14/raw_session_{session_id}_"))
            self.assertTrue(summary_key.startswith(f"landing/date=2026-09-14/session_{session_id}_summary_"))
            self.assertTrue(manifest_key.startswith(f"landing/date=2026-09-14/recording_{session_id}_manifest_"))
            self.assertEqual(mock_s3.upload_file.call_count, 3)

    def test_upload_session_fallback_to_file_mtime(self):
        """Verifies that when summary is missing, file mtime (UTC) is used as the partition date."""
        session_id = "test-session-mtime"
        raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps({"gameData": {"gameTime": 50}}) + "\n" * 10)

        # Set specific mtime to 2026-08-20 12:00:00 UTC (timestamp: 1787227200)
        target_timestamp = 1787227200.0
        os.utime(raw_path, (target_timestamp, target_timestamp))

        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            result = self.archiver.upload_session_to_landing(session_id=session_id)

            self.assertEqual(result["status"], "success")
            expected_date = datetime.utcfromtimestamp(target_timestamp).strftime("%Y-%m-%d")
            self.assertTrue(
                result["uploaded_keys"][0].startswith(f"landing/date={expected_date}/raw_session_{session_id}_")
            )

    def test_upload_session_empty_skipped(self):
        """Verifies that empty/non-existent session files are skipped gracefully."""
        with patch("boto3.client") as mock_boto:
            result = self.archiver.upload_session_to_landing(session_id="non-existent-uuid")
        self.assertEqual(result["status"], "skipped")
        mock_boto.assert_not_called()

    def test_corrupt_gzip_is_rejected_before_upload(self):
        session_id = "corrupt-session"
        raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        with open(raw_path, "wb") as f:
            f.write(b"not-a-gzip-file" * 10)

        with patch("boto3.client") as mock_boto:
            result = self.archiver.upload_session_to_landing(session_id=session_id)

        self.assertEqual(result["status"], "error")
        mock_boto.assert_not_called()

    def test_versioned_envelope_is_reflected_in_manifest(self):
        session_id = "versioned-session"
        raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        payload_json = json.dumps(
            {
                "gameData": {"gameTime": 12.5, "gameMode": "CLASSIC"},
                "activePlayer": {"riotId": "Player#EUW"},
                "allPlayers": [{"riotId": "Player#EUW", "team": "ORDER"}],
                "events": {"Events": [{"EventName": "GameEnd", "Result": "Win"}]},
            }
        )
        envelope = {
            "schema_version": "1.0",
            "recording_id": session_id,
            "observation_id": "observation-1",
            "sequence_no": 1,
            "observed_at_utc": "2026-09-15T12:00:00Z",
            "collector_version": "test",
            "payload_sha256": "test-hash",
            "payload_json": payload_json,
        }
        with gzip.open(raw_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps(envelope) + "\n")

        with patch("boto3.client"):
            result = self.archiver.upload_session_to_landing(session_id=session_id)

        self.assertEqual(result["status"], "success")
        manifest_path = os.path.join(self.data_dir, f"recording_{session_id}_manifest.json")
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
        self.assertEqual(manifest["raw"]["schema_versions"], ["1.0"])
        self.assertEqual(manifest["raw"]["collector_versions"], ["test"])
        self.assertEqual(manifest["raw"]["game_modes"], ["CLASSIC"])
        self.assertEqual(manifest["observed_winner"], "BLUE")
        self.assertEqual(manifest["outcome_status"], "OBSERVED")

    def test_upload_all_sessions_reports_batch_result(self):
        for session_id in ("session-a", "session-b"):
            raw_path = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
            with gzip.open(raw_path, "wt", encoding="utf-8") as f:
                f.write(json.dumps({"gameData": {"gameTime": 1}}) + "\n")

        with patch.object(
            self.archiver,
            "upload_session_to_landing",
            side_effect=[{"status": "success"}, {"status": "error"}],
        ) as upload:
            result = self.archiver.upload_all_sessions()

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["session_count"], 2)
        self.assertEqual(result["success_count"], 1)
        self.assertEqual(result["error_count"], 1)
        self.assertEqual([call.args[0] for call in upload.call_args_list], ["session-a", "session-b"])


class TestDatabricksAutoLoaderScript(unittest.TestCase):
    def setUp(self):
        self.script_paths = [
            "lakehouse/notebooks/01_landing_to_bronze.py",
            "lakehouse/notebooks/02_bronze_to_silver.py",
        ]

    def test_scripts_exist(self):
        """Ensure the canonical PySpark Auto Loader notebook exists."""
        for p in self.script_paths:
            self.assertTrue(os.path.exists(p), f"Script missing at {p}")

    def test_script_syntax_and_structure(self):
        """Validates Python AST and architectural requirements of 01_landing_to_bronze.py."""
        with open("lakehouse/notebooks/01_landing_to_bronze.py", encoding="utf-8") as f:
            code = f.read()

        # Check that it parses into a valid Python AST
        tree = ast.parse(code)
        self.assertIsNotNone(tree)

        # Architectural assertions:
        # 1. Format is cloudFiles (Databricks Auto Loader)
        self.assertIn('format("cloudFiles")', code)
        self.assertIn('option("cloudFiles.format", "text")', code)
        self.assertIn('option("pathGlobFilter", path_glob)', code)
        self.assertIn('path_glob="raw_session_*.jsonl.gz"', code)
        self.assertIn('path_glob="recording_*_manifest_*.json"', code)
        self.assertIn('F.col("value").alias("_raw_json")', code)

        # 2. Audit metadata comes from the supported hidden metadata column.
        self.assertIn("_ingested_at", code)
        self.assertIn('F.col("_metadata.file_path")', code)
        self.assertIn("landing_date", code)
        self.assertIn('r"/date=(\\d{4}-\\d{2}-\\d{2})/"', code)
        self.assertIn("F.to_date", code)
        self.assertIn("_recording_id_from_filename", code)
        self.assertIn('environment = get_parameter("environment", "prod")', code)
        self.assertNotIn("F.input_file_name()", code)
        self.assertNotIn('F.col("gameCreation")', code)

        # 3. Flat external Delta tables under bronze/.
        self.assertIn('format("delta")', code)
        self.assertIn('outputMode("append")', code)
        self.assertIn("CREATE TABLE IF NOT EXISTS", code)
        self.assertIn("LOCATION '{target_path}'", code)
        self.assertIn("trigger(availableNow=True)", code)
        self.assertIn("toTable(table_name)", code)
        self.assertNotIn(
            ".partitionBy(",
            code,
            "Bronze writeStream must remain flat (no .partitionBy() call) to prevent small files!",
        )

        # 4. The small daily volume does not justify unconditional compaction.
        self.assertNotIn('spark.sql(f"OPTIMIZE', code)
        self.assertIn("awaitTermination()", code)

    def test_silver_script_models_only_balance_inputs(self):
        with open("lakehouse/notebooks/02_bronze_to_silver.py", encoding="utf-8") as f:
            code = f.read()

        ast.parse(code)
        self.assertIn('F.from_json("_raw_json", envelope_schema)', code)
        self.assertIn('F.from_json("payload_json", payload_schema)', code)
        self.assertIn('F.lit("legacy")', code)
        self.assertIn("OBSERVATION_ID_CONFLICT", code)
        self.assertIn('replace_silver_table(observations, "observations")', code)
        self.assertIn('replace_silver_table(matches, "matches")', code)
        self.assertIn('replace_silver_table(match_participants, "match_participants")', code)
        self.assertIn('replace_silver_table(match_participant_items, "match_participant_items")', code)
        self.assertIn('replace_silver_table(quarantine, "quarantine_observations")', code)
        self.assertIn("is_balance_eligible", code)
        self.assertIn("is_outcome_confirmed", code)
        self.assertNotIn("predicted_winner).alias", code)


class TestAirflowOrchestrationDAG(unittest.TestCase):
    def test_daily_lakehouse_ingest_dag_file(self):
        """Validates daily_lakehouse_ingest_dag.py configuration and AST."""
        dag_path = "orchestration/dags/daily_lakehouse_ingest_dag.py"
        self.assertTrue(os.path.exists(dag_path))
        with open(dag_path, encoding="utf-8") as f:
            code = f.read()

        ast.parse(code)
        self.assertIn('dag_id="daily_lakehouse_ingest"', code)
        self.assertIn('schedule="0 1 * * *"', code)
        self.assertIn('"owner": "data_engineering"', code)
        self.assertIn('"retries": 2', code)
        self.assertIn("tz=WARSAW", code)
        self.assertIn("DatabricksRunNowOperator", code)
        self.assertIn('databricks_conn_id="databricks_default"', code)
        self.assertIn('job_id="{{ var.value.databricks_job_id }}"', code)
        self.assertIn("job_parameters=", code)
        self.assertIn("idempotency_token=", code)
        self.assertIn("wait_for_termination=True", code)

    def test_docker_compose_databricks_provider(self):
        """Validate the pinned image and environment-provisioned Airflow connection."""
        compose_path = "orchestration/docker-compose.yaml"
        with open(compose_path, encoding="utf-8") as f:
            content = f.read()

        with open("orchestration/requirements.txt", encoding="utf-8") as f:
            requirements = f.read()
        with open("orchestration/Dockerfile", encoding="utf-8") as f:
            dockerfile = f.read()
        with open("orchestration/.dockerignore", encoding="utf-8") as f:
            dockerignore = f.read()

        self.assertIn("apache-airflow-providers-databricks==7.0.0", requirements)
        self.assertIn("constraints-${PYTHON_VERSION}.txt", dockerfile)
        self.assertNotIn("constraints-no-providers", dockerfile)
        self.assertIn("AIRFLOW_CONN_DATABRICKS_DEFAULT", content)
        self.assertIn("AIRFLOW_VAR_DATABRICKS_JOB_ID", content)
        self.assertIn("127.0.0.1:8080:8080", content)
        self.assertNotIn("AWS_ACCESS_KEY_ID", content)
        self.assertIn(".env", dockerignore.splitlines())

    def test_databricks_bundle_defines_unscheduled_single_job(self):
        """The bundle deploys compute; Airflow remains the only scheduler."""
        with open("databricks.yml", encoding="utf-8") as f:
            bundle = f.read()
        with open("resources/landing_to_bronze.job.yml", encoding="utf-8") as f:
            job = f.read()

        self.assertIn("resources/*.yml", bundle)
        self.assertIn("s3_bucket:", bundle)
        self.assertIn("prod:", bundle)
        self.assertIn("mode: production", bundle)
        self.assertIn(
            "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}",
            bundle,
        )
        self.assertNotIn("dev:", bundle)
        self.assertIn("landing_to_bronze:", job)
        self.assertIn("max_concurrent_runs: 1", job)
        self.assertIn("01_landing_to_bronze.py", job)
        self.assertIn("02_bronze_to_silver.py", job)
        self.assertIn("transform_bronze_to_silver", job)
        self.assertIn("depends_on:", job)
        self.assertNotIn("schedule:", job)


if __name__ == "__main__":
    unittest.main()
