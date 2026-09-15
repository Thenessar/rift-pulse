import argparse
import glob
import gzip
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from services.outcomes import extract_observed_outcome

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rift-pulse.archiver")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_dotenv_simple(env_path: str = ".env"):
    """Loads environment variables from .env if present."""
    if not os.path.exists(env_path):
        alt_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
        if os.path.exists(alt_path):
            env_path = alt_path
        else:
            return

    try:
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception as e:
        logger.debug(f"Failed to load environment file {env_path}: {e}")


class S3Archiver:
    def __init__(self, data_dir: str = "data/matches"):
        load_dotenv_simple()
        self.data_dir = data_dir

        self.bucket_name = os.environ.get("AWS_S3_BUCKET", "rift-pulse-data-lake")
        self.region_name = os.environ.get("AWS_REGION", "eu-central-1")

    def _inspect_raw_file(self, raw_file: str, session_id: str) -> dict[str, Any]:
        """Validate gzip/JSONL and produce authoritative manifest statistics."""
        record_count = 0
        schema_versions: set[str] = set()
        collector_versions: set[str] = set()
        game_modes: set[str] = set()
        min_game_time: float | None = None
        max_game_time: float | None = None
        first_observed_at: str | None = None
        last_observed_at: str | None = None
        observed_outcome = {"observed_winner": None, "outcome_status": "UNKNOWN", "outcome_source": None}

        with gzip.open(raw_file, "rt", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON at raw line {line_number}: {exc}") from exc

                record_count += 1
                if record.get("schema_version") and record.get("payload_json") is not None:
                    if record.get("recording_id") != session_id:
                        raise ValueError(
                            f"Envelope recording_id mismatch at line {line_number}: "
                            f"{record.get('recording_id')!r} != {session_id!r}"
                        )
                    schema_versions.add(str(record["schema_version"]))
                    if record.get("collector_version"):
                        collector_versions.add(str(record["collector_version"]))
                    observed_at = record.get("observed_at_utc")
                    first_observed_at = first_observed_at or observed_at
                    last_observed_at = observed_at or last_observed_at
                    try:
                        payload = json.loads(record["payload_json"])
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise ValueError(f"Invalid payload_json at raw line {line_number}: {exc}") from exc
                else:
                    schema_versions.add("legacy")
                    payload = record

                game_data = payload.get("gameData") or {}
                candidate_outcome = extract_observed_outcome(payload)
                if candidate_outcome["outcome_status"] == "OBSERVED":
                    observed_outcome = candidate_outcome
                game_mode = game_data.get("gameMode")
                if game_mode:
                    game_modes.add(str(game_mode))
                game_time = game_data.get("gameTime")
                if isinstance(game_time, (int, float)):
                    value = float(game_time)
                    min_game_time = value if min_game_time is None else min(min_game_time, value)
                    max_game_time = value if max_game_time is None else max(max_game_time, value)

        if record_count == 0:
            raise ValueError("Raw gzip contains no JSON records")

        return {
            "file_name": os.path.basename(raw_file),
            "sha256": sha256_file(raw_file),
            "size_bytes": os.path.getsize(raw_file),
            "record_count": record_count,
            "schema_versions": sorted(schema_versions),
            "collector_versions": sorted(collector_versions),
            "first_observed_at_utc": first_observed_at,
            "last_observed_at_utc": last_observed_at,
            "min_game_time_seconds": min_game_time,
            "max_game_time_seconds": max_game_time,
            "game_modes": sorted(game_modes),
            **observed_outcome,
        }

    def _build_manifest(
        self,
        session_id: str,
        raw_file: str,
        summary_file: str,
    ) -> tuple[str, dict[str, Any]]:
        raw_stats = self._inspect_raw_file(raw_file, session_id)
        summary: dict[str, Any] = {}
        if os.path.exists(summary_file):
            with open(summary_file, encoding="utf-8") as source:
                summary = json.load(source)

        stable_created_at = (
            summary.get("recorded_at")
            or raw_stats.get("last_observed_at_utc")
            or datetime.fromtimestamp(os.path.getmtime(raw_file), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        )

        manifest = {
            "manifest_schema_version": "1.0",
            "recording_id": session_id,
            "source": "riot_live_client",
            "created_at_utc": stable_created_at,
            "completion_status": "COMPLETE_UNVERIFIED" if summary else "INCOMPLETE",
            "recorded_at_utc": summary.get("recorded_at"),
            "raw": raw_stats,
            "reported_total_ticks": summary.get("total_ticks"),
            "record_count_matches_summary": (
                summary.get("total_ticks") == raw_stats["record_count"]
                if summary.get("total_ticks") is not None
                else None
            ),
            "observed_winner": summary.get("observed_winner") or raw_stats.get("observed_winner"),
            "outcome_status": (
                summary.get("outcome_status")
                if summary.get("outcome_status") not in {None, "UNKNOWN"}
                else raw_stats.get("outcome_status", "UNKNOWN")
            ),
            "outcome_source": summary.get("outcome_source") or raw_stats.get("outcome_source"),
            "predicted_winner": summary.get("predicted_winner"),
        }
        manifest_path = os.path.join(self.data_dir, f"recording_{session_id}_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as target:
            json.dump(manifest, target, ensure_ascii=False, separators=(",", ":"))
            target.write("\n")
        return manifest_path, manifest

    def upload_session_to_landing(self, session_id: str, game_end_date: str | None = None) -> dict[str, Any]:
        """
        Uploads raw match telemetry and summary metadata to S3 landing/ prefix.
        Target paths:
          s3://<bucket>/landing/date={YYYY-MM-DD}/raw_session_<session_id>.jsonl.gz
          s3://<bucket>/landing/date={YYYY-MM-DD}/session_<session_id>_summary.json
        """
        raw_file = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        summary_file = os.path.join(self.data_dir, f"session_{session_id}_summary.json")

        has_raw = os.path.exists(raw_file) and os.path.getsize(raw_file) > 80
        has_summary = os.path.exists(summary_file)
        if not has_raw:
            logger.warning(f"No non-empty files found for session: {session_id}")
            return {"status": "skipped", "message": "No session files found"}

        try:
            manifest_file, manifest = self._build_manifest(session_id, raw_file, summary_file)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            logger.error("Raw validation failed for session %s: %s", session_id, exc)
            return {"status": "error", "message": str(exc), "session_id": session_id}

        # Determine partition date (from game_end_date, summary recorded_at, raw file mtime, or current UTC date)
        match_date = game_end_date
        if not match_date and os.path.exists(summary_file):
            try:
                with open(summary_file, encoding="utf-8") as f:
                    summary_data = json.load(f)
                    recorded_at = summary_data.get("recorded_at")
                    if recorded_at and len(recorded_at) >= 10:
                        match_date = recorded_at[:10]
            except Exception as e:
                logger.debug(f"Could not parse recorded_at from {summary_file}: {e}")

        if not match_date and os.path.exists(raw_file):
            try:
                mtime = os.path.getmtime(raw_file)
                match_date = datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%d")
            except Exception as e:
                logger.debug(f"Could not determine mtime from {raw_file}: {e}")

        if not match_date:
            match_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        try:
            datetime.strptime(match_date, "%Y-%m-%d")
        except (TypeError, ValueError):
            return {"status": "error", "message": f"Invalid game_end_date: {match_date!r}", "session_id": session_id}

        landing_prefix = f"landing/date={match_date}"
        uploaded = []
        try:
            s3_client = boto3.client("s3", region_name=self.region_name)

            # Content-addressed keys make retries idempotent and prevent a changed
            # recording from overwriting an object already discovered by Auto Loader.
            raw_hash = manifest["raw"]["sha256"]
            key = f"{landing_prefix}/raw_session_{session_id}_{raw_hash[:16]}.jsonl.gz"
            file_size_kb = os.path.getsize(raw_file) / 1024.0
            logger.info(f"Uploading raw file to s3://{self.bucket_name}/{key} ({file_size_kb:.1f} KB)")
            s3_client.upload_file(
                Filename=raw_file, Bucket=self.bucket_name, Key=key, ExtraArgs={"ContentType": "application/gzip"}
            )
            uploaded.append(key)

            # 2. Upload match summary metadata
            if has_summary:
                summary_hash = sha256_file(summary_file)
                sum_key = f"{landing_prefix}/session_{session_id}_summary_{summary_hash[:16]}.json"
                logger.info(f"Uploading session summary to s3://{self.bucket_name}/{sum_key}")
                s3_client.upload_file(
                    Filename=summary_file,
                    Bucket=self.bucket_name,
                    Key=sum_key,
                    ExtraArgs={"ContentType": "application/json"},
                )
                uploaded.append(sum_key)

            # The manifest is the commit marker and must be uploaded last.
            manifest_hash = sha256_file(manifest_file)
            manifest_key = f"{landing_prefix}/recording_{session_id}_manifest_{manifest_hash[:16]}.json"
            logger.info(f"Uploading recording manifest to s3://{self.bucket_name}/{manifest_key}")
            s3_client.upload_file(
                Filename=manifest_file,
                Bucket=self.bucket_name,
                Key=manifest_key,
                ExtraArgs={"ContentType": "application/json"},
            )
            uploaded.append(manifest_key)

            logger.info(f"Successfully uploaded session {session_id} to S3 landing: {uploaded}")
            return {
                "status": "success",
                "session_id": session_id,
                "bucket": self.bucket_name,
                "uploaded_keys": uploaded,
                "timestamp": utc_now_iso(),
            }

        except NoCredentialsError:
            logger.warning("AWS credentials not found. Configure .env or AWS CLI. Skipping upload.")
            return {"status": "error", "message": "Missing AWS credentials"}
        except ClientError as e:
            logger.error(f"S3 client error while uploading session {session_id}: {e}")
            return {"status": "error", "message": str(e)}
        except Exception as e:
            logger.error(f"Unexpected error while uploading session {session_id}: {e}")
            return {"status": "error", "message": str(e)}

    def upload_all_sessions(self) -> dict[str, Any]:
        """Validate and idempotently upload every locally retained raw session."""
        results = []
        pattern = os.path.join(self.data_dir, "raw_session_*.jsonl.gz")
        for raw_file in sorted(glob.glob(pattern)):
            file_name = os.path.basename(raw_file)
            session_id = file_name.removeprefix("raw_session_").removesuffix(".jsonl.gz")
            results.append(self.upload_session_to_landing(session_id))

        return {
            "status": "success" if all(result.get("status") != "error" for result in results) else "error",
            "session_count": len(results),
            "success_count": sum(result.get("status") == "success" for result in results),
            "error_count": sum(result.get("status") == "error" for result in results),
            "results": results,
        }

    def test_s3_connection(self) -> dict[str, Any]:
        """Validates S3 connectivity and s3:PutObject permissions in landing/."""
        test_key = "landing/.ping_test.json"
        test_data = json.dumps(
            {"test": True, "project": "rift-pulse", "tested_at": datetime.utcnow().isoformat()}
        ).encode("utf-8")

        try:
            s3_client = boto3.client("s3", region_name=self.region_name)
            logger.info(f"Testing s3:PutObject on s3://{self.bucket_name}/{test_key}")

            s3_client.put_object(Bucket=self.bucket_name, Key=test_key, Body=test_data, ContentType="application/json")
            logger.info(f"S3 connection test for bucket '{self.bucket_name}' succeeded.")
            return {"status": "success", "message": f"Successfully wrote to s3://{self.bucket_name}/{test_key}"}
        except NoCredentialsError:
            msg = "Missing AWS credentials. Ensure .env contains AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
            logger.error(msg)
            return {"status": "error", "message": msg}
        except ClientError as e:
            logger.error(f"S3 access error: {e}")
            return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rift-Pulse S3 Archiver & Uploader")
    parser.add_argument("--test", action="store_true", help="Test S3 connectivity and landing/ upload permission")
    parser.add_argument("--all", action="store_true", help="Validate and upload every local raw session")
    parser.add_argument("--session", type=str, help="Session UUID to upload to landing/")
    parser.add_argument("--date", type=str, help="Target batch date (YYYY-MM-DD)")
    args = parser.parse_args()

    archiver = S3Archiver()

    if args.test:
        res = archiver.test_s3_connection()
        print(json.dumps(res, indent=2))
    elif args.all:
        res = archiver.upload_all_sessions()
        print(json.dumps(res, indent=2))
    elif args.session:
        res = archiver.upload_session_to_landing(args.session, game_end_date=args.date)
        print(json.dumps(res, indent=2))
    else:
        print("Usage: python -m services.archiver.s3_archiver --test | --all | --session <uuid> [--date YYYY-MM-DD]")
