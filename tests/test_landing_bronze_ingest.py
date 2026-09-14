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
            expected_key = f"landing/date=2026-09-15/raw_session_{session_id}.jsonl.gz"
            self.assertIn(expected_key, result["uploaded_keys"])

            # Verify boto3 call arguments
            mock_s3.upload_file.assert_called_once_with(
                Filename=raw_path,
                Bucket=self.archiver.bucket_name,
                Key=expected_key,
                ExtraArgs={"ContentType": "application/gzip"},
            )

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
            expected_raw_key = f"landing/date=2026-09-14/raw_session_{session_id}.jsonl.gz"
            expected_sum_key = f"landing/date=2026-09-14/session_{session_id}_summary.json"
            self.assertIn(expected_raw_key, result["uploaded_keys"])
            self.assertIn(expected_sum_key, result["uploaded_keys"])
            self.assertEqual(mock_s3.upload_file.call_count, 2)

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
            expected_key = f"landing/date={expected_date}/raw_session_{session_id}.jsonl.gz"
            self.assertIn(expected_key, result["uploaded_keys"])

    def test_upload_session_empty_skipped(self):
        """Verifies that empty/non-existent session files are skipped gracefully."""
        with patch("boto3.client") as mock_boto:
            result = self.archiver.upload_session_to_landing(session_id="non-existent-uuid")
        self.assertEqual(result["status"], "skipped")
        mock_boto.assert_not_called()


class TestDatabricksAutoLoaderScript(unittest.TestCase):
    def setUp(self):
        self.script_paths = [
            "lakehouse/notebooks/01_landing_to_bronze.py",
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
        self.assertIn('option("cloudFiles.format", "json")', code)
        self.assertIn('option("cloudFiles.schemaLocation", schema_path)', code)
        self.assertIn('option("cloudFiles.inferColumnTypes", "true")', code)
        self.assertIn('option("cloudFiles.schemaEvolutionMode", "addNewColumns")', code)
        self.assertIn('option("rescuedDataColumn", "_rescued_data")', code)
        self.assertIn('option("pathGlobFilter", "raw_session_*.jsonl.gz")', code)

        # 2. Audit metadata comes from the supported hidden metadata column.
        self.assertIn("_ingested_at", code)
        self.assertIn('F.col("_metadata.file_path")', code)
        self.assertIn("landing_date", code)
        self.assertIn("_recording_id_from_filename", code)
        self.assertIn('environment = "prod"', code)
        self.assertNotIn("F.input_file_name()", code)
        self.assertNotIn('F.col("gameCreation")', code)

        # 3. Flat external Delta table under bronze/, with schema evolution enabled.
        self.assertIn('format("delta")', code)
        self.assertIn('outputMode("append")', code)
        self.assertIn('option("mergeSchema", "true")', code)
        self.assertIn("CREATE TABLE IF NOT EXISTS", code)
        self.assertIn("LOCATION '{bronze_path}'", code)
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


class TestAirflowOrchestrationDAG(unittest.TestCase):
    def test_daily_lakehouse_ingest_dag_file(self):
        """Validates daily_lakehouse_ingest_dag.py configuration and AST."""
        dag_path = "orchestration/dags/daily_lakehouse_ingest_dag.py"
        self.assertTrue(os.path.exists(dag_path))
        with open(dag_path, encoding="utf-8") as f:
            code = f.read()

        ast.parse(code)
        self.assertIn('dag_id="daily_landing_to_bronze"', code)
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
        with open("orchestration/.dockerignore", encoding="utf-8") as f:
            dockerignore = f.read()

        self.assertIn("apache-airflow-providers-databricks==7.5.0", requirements)
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
        self.assertNotIn("schedule:", job)


if __name__ == "__main__":
    unittest.main()
