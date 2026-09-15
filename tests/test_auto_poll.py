import asyncio
import gzip
import json
import os
import tempfile
import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from services.engine.src.main import app
from services.engine.src.main import engine as main_engine
from services.engine.src.poller import TelemetryEngine
from services.outcomes import extract_observed_outcome

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixtures", "sample_live_data.json")


class TestObservedOutcome(unittest.TestCase):
    def test_game_end_win_maps_exact_active_player_team(self):
        raw = {
            "activePlayer": {"riotId": "Player#EUW"},
            "allPlayers": [
                {"riotId": "Player#EUW", "team": "ORDER"},
                {"riotId": "Opponent#EUW", "team": "CHAOS"},
            ],
            "events": {"Events": [{"EventName": "GameEnd", "Result": "Win"}]},
        }

        self.assertEqual(
            extract_observed_outcome(raw),
            {
                "observed_winner": "BLUE",
                "outcome_status": "OBSERVED",
                "outcome_source": "riot_live_client_game_end",
            },
        )

    def test_game_end_loss_maps_opposing_team(self):
        raw = {
            "activePlayer": {"riotIdGameName": "Player", "riotIdTagLine": "EUW"},
            "allPlayers": [{"riotId": "Player#EUW", "team": "CHAOS"}],
            "events": {"Events": [{"EventName": "GameEnd", "Result": "Lose"}]},
        }

        self.assertEqual(extract_observed_outcome(raw)["observed_winner"], "BLUE")

    def test_missing_game_end_stays_unknown(self):
        self.assertEqual(extract_observed_outcome({})["outcome_status"], "UNKNOWN")


class TestAutoPollIdle(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)

    def tearDown(self):
        with suppress(Exception):
            self.temp_dir.cleanup()

    def test_default_config_auto_poll_true(self):
        """Verify that auto_poll_idle defaults to True and interval defaults to 2.5s."""
        engine = TelemetryEngine(model_path="services/engine/models/model.onnx", data_dir=self.temp_dir.name)
        self.assertTrue(engine.auto_poll_idle)
        self.assertEqual(engine.idle_poll_interval, 2.5)

    def test_env_var_override(self):
        """Verify that AUTO_POLL_IDLE and IDLE_POLL_INTERVAL environment variables take effect."""
        with patch.dict(os.environ, {"AUTO_POLL_IDLE": "false", "IDLE_POLL_INTERVAL": "5.0"}):
            engine = TelemetryEngine(model_path="services/engine/models/model.onnx", data_dir=self.temp_dir.name)
            self.assertFalse(engine.auto_poll_idle)
            self.assertEqual(engine.idle_poll_interval, 5.0)

    def test_set_auto_poll_method(self):
        """Verify that set_auto_poll dynamically toggles state and interval."""
        engine = TelemetryEngine(
            model_path="services/engine/models/model.onnx", data_dir=self.temp_dir.name, auto_poll_idle=False
        )
        self.assertFalse(engine.auto_poll_idle)

        engine.set_auto_poll(True, interval=1.8)
        self.assertTrue(engine.auto_poll_idle)
        self.assertEqual(engine.idle_poll_interval, 1.8)

        engine.set_auto_poll(False)
        self.assertFalse(engine.auto_poll_idle)
        self.assertEqual(engine.idle_poll_interval, 1.8)

    def test_s3_upload_retries_transient_failure(self):
        """Verify that a failed post-match upload is retried without contacting AWS."""
        engine = TelemetryEngine(
            model_path="services/engine/models/model.onnx",
            data_dir=self.temp_dir.name,
        )

        results = [
            {"status": "error", "message": "temporary outage"},
            {"status": "success"},
        ]
        with (
            patch.dict(
                os.environ,
                {"S3_UPLOAD_MAX_ATTEMPTS": "3", "S3_UPLOAD_RETRY_DELAY": "0"},
            ),
            patch(
                "services.archiver.s3_archiver.S3Archiver.upload_session_to_landing",
                side_effect=results,
            ) as upload,
            patch("services.engine.src.poller.time.sleep") as sleep,
        ):
            engine._upload_session_with_retries("session-id", "2026-09-14")

        self.assertEqual(upload.call_count, 2)
        sleep.assert_called_once_with(0.0)

    def test_auto_detect_game_from_second_zero(self):
        """Verify that poll_loop automatically detects a match starting at 0:00, initializes session, and persists tick 1."""
        engine = TelemetryEngine(
            model_path="services/engine/models/model.onnx",
            data_dir=self.temp_dir.name,
            auto_poll_idle=True,
            idle_poll_interval=0.05,
        )

        with open(FIXTURE_PATH, encoding="utf-8-sig") as f:
            sample_data = json.load(f)

        # Set game time to 0.0 seconds to test registration from second 0:00
        sample_data["gameData"]["gameTime"] = 0.0

        async def run_test():
            # Mock httpx response
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = sample_data

            with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
                mock_get.return_value = mock_resp

                # Start poll_loop task
                poll_task = asyncio.create_task(engine.poll_loop())

                # Wait briefly for poll_loop to execute at least one iteration
                for _ in range(20):
                    if engine.is_game_active and engine.tick_counter >= 1:
                        break
                    await asyncio.sleep(0.05)

                active_during_run = engine.is_game_active
                status_during_run = engine.game_status
                ticks_during_run = engine.tick_counter
                session_id = engine.session_id

                engine.stop()
                poll_task.cancel()
                with suppress(asyncio.CancelledError):
                    await poll_task

            self.assertTrue(active_during_run)
            self.assertEqual(status_during_run, "IN_GAME")
            self.assertGreaterEqual(ticks_during_run, 1)
            self.assertIsNotNone(engine.latest_data)
            self.assertEqual(engine.latest_data.get("game_time_formatted"), "00:00")
            self.assertEqual(engine.game_status, "MATCH_COMPLETED")

            # Check that files were created
            norm_file = os.path.join(self.temp_dir.name, f"session_{session_id}.jsonl")
            raw_file = os.path.join(self.temp_dir.name, f"raw_session_{session_id}.jsonl.gz")
            summary_file = os.path.join(self.temp_dir.name, f"session_{session_id}_summary.json")
            self.assertTrue(os.path.exists(norm_file), f"Normalized session file missing: {norm_file}")
            self.assertTrue(os.path.exists(raw_file), f"Raw True Bronze file missing: {raw_file}")
            self.assertTrue(os.path.exists(summary_file), f"Summary file missing: {summary_file}")

            with gzip.open(raw_file, "rt", encoding="utf-8") as f:
                envelope = json.loads(next(line for line in f if line.strip()))
            self.assertEqual(envelope["schema_version"], "1.0")
            self.assertEqual(envelope["recording_id"], session_id)
            self.assertTrue(envelope["observation_id"])
            self.assertGreaterEqual(envelope["sequence_no"], 1)
            self.assertTrue(envelope["observed_at_utc"].endswith("Z"))
            self.assertEqual(json.loads(envelope["payload_json"])["gameData"], sample_data["gameData"])

            with open(summary_file, encoding="utf-8") as f:
                summary = json.load(f)
            self.assertIsNone(summary["observed_winner"])
            self.assertEqual(summary["outcome_status"], "UNKNOWN")

        with patch(
            "services.archiver.s3_archiver.S3Archiver.upload_session_to_landing",
            return_value={"status": "success"},
        ):
            asyncio.run(run_test())

    def test_silent_idle_when_client_down(self):
        """Verify that when game client is offline, idle poll loop stays quiet without crashing or activating game."""
        engine = TelemetryEngine(
            model_path="services/engine/models/model.onnx",
            data_dir=self.temp_dir.name,
            auto_poll_idle=True,
            idle_poll_interval=0.05,
        )

        async def run_test():
            with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
                mock_get.side_effect = Exception("Connection refused to 127.0.0.1:2999")

                poll_task = asyncio.create_task(engine.poll_loop())
                await asyncio.sleep(0.15)

                engine.stop()
                poll_task.cancel()
                with suppress(asyncio.CancelledError):
                    await poll_task

            self.assertFalse(engine.is_game_active)
            self.assertEqual(engine.tick_counter, 0)

        asyncio.run(run_test())

    def test_api_status_and_auto_poll_toggle(self):
        """Verify that GET /api/v1/live/status and POST /api/v1/live/auto-poll return and update auto-poll config."""
        client = TestClient(app)

        # GET /api/v1/live/status
        resp = client.get("/api/v1/live/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("auto_poll_idle", data)
        self.assertIn("idle_poll_interval", data)

        # POST /api/v1/live/auto-poll toggle
        resp_post = client.post("/api/v1/live/auto-poll", json={"enabled": False, "interval": 4.0})
        self.assertEqual(resp_post.status_code, 200)
        post_data = resp_post.json()
        self.assertFalse(post_data["auto_poll_idle"])
        self.assertEqual(post_data["idle_poll_interval"], 4.0)
        self.assertFalse(main_engine.auto_poll_idle)

        # Restore
        client.post("/api/v1/live/auto-poll", json={"enabled": True, "interval": 2.5})
        self.assertTrue(main_engine.auto_poll_idle)

    def test_game_exit_finalization_and_resume_idle(self):
        """Verify that when a match ends (4 failures), engine finalizes session and resumes idle auto-poll for next game."""
        engine = TelemetryEngine(
            model_path="services/engine/models/model.onnx",
            data_dir=self.temp_dir.name,
            auto_poll_idle=True,
            idle_poll_interval=0.02,
            active_poll_interval=0.02,
        )

        with open(FIXTURE_PATH, encoding="utf-8-sig") as f:
            sample_data = json.load(f)

        sample_data_2 = json.loads(json.dumps(sample_data))
        sample_data_2["gameData"]["gameTime"] = 0.0

        async def run_test():
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = sample_data

            mock_resp_2 = MagicMock()
            mock_resp_2.status_code = 200
            mock_resp_2.json.return_value = sample_data_2

            call_count = 0

            async def mock_get(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                # First 2 calls: game 1 is active
                if call_count <= 2:
                    return mock_resp
                # Next 6 calls: game 1 exited (failure)
                elif call_count <= 8:
                    raise Exception("LoL process exited")
                # Subsequent calls: game 2 started at 0:00!
                else:
                    return mock_resp_2

            with patch("httpx.AsyncClient.get", side_effect=mock_get):
                poll_task = asyncio.create_task(engine.poll_loop())
                try:
                    # Wait for match 1 to be active
                    for _ in range(30):
                        if engine.is_game_active:
                            break
                        await asyncio.sleep(0.02)
                    self.assertTrue(engine.is_game_active)
                    first_session_id = engine.session_id

                    # Wait for match 1 to finalize after 4 failures
                    for _ in range(40):
                        if not engine.is_game_active and engine.game_status == "MATCH_COMPLETED":
                            break
                        await asyncio.sleep(0.02)
                    self.assertFalse(engine.is_game_active)
                    self.assertEqual(engine.game_status, "MATCH_COMPLETED")

                    # Wait for match 2 to be automatically detected
                    for _ in range(40):
                        if engine.is_game_active and engine.session_id != first_session_id:
                            break
                        await asyncio.sleep(0.02)

                    self.assertTrue(engine.is_game_active)
                    self.assertNotEqual(engine.session_id, first_session_id)
                    self.assertEqual(engine.game_status, "IN_GAME")
                finally:
                    engine.stop()
                    poll_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await poll_task

        with patch(
            "services.archiver.s3_archiver.S3Archiver.upload_session_to_landing",
            return_value={"status": "success"},
        ):
            asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
