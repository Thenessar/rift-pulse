import argparse
import json
import logging
import os
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rift-pulse.archiver")


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

    def upload_session_to_landing(self, session_id: str, game_end_date: str | None = None) -> dict[str, Any]:
        """
        Uploads raw match telemetry and summary metadata to S3 landing/ prefix.
        Target paths:
          s3://<bucket>/landing/date={YYYY-MM-DD}/raw_session_<session_id>.jsonl.gz
          s3://<bucket>/landing/date={YYYY-MM-DD}/session_<session_id>_summary.json
        """
        raw_file = os.path.join(self.data_dir, f"raw_session_{session_id}.jsonl.gz")
        norm_file = os.path.join(self.data_dir, f"session_{session_id}.jsonl")
        summary_file = os.path.join(self.data_dir, f"session_{session_id}_summary.json")

        has_raw = os.path.exists(raw_file) and os.path.getsize(raw_file) > 80
        has_normalized = os.path.exists(norm_file) and os.path.getsize(norm_file) > 0
        has_summary = os.path.exists(summary_file)
        if not has_raw and not has_normalized and not has_summary:
            logger.warning(f"No non-empty files found for session: {session_id}")
            return {"status": "skipped", "message": "No session files found"}

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
                match_date = datetime.utcfromtimestamp(mtime).strftime("%Y-%m-%d")
            except Exception as e:
                logger.debug(f"Could not determine mtime from {raw_file}: {e}")

        if not match_date:
            match_date = datetime.utcnow().strftime("%Y-%m-%d")

        landing_prefix = f"landing/date={match_date}"
        uploaded = []
        try:
            s3_client = boto3.client("s3", region_name=self.region_name)

            # 1. Upload raw GZIP telemetry (True Bronze)
            if has_raw:
                key = f"{landing_prefix}/{os.path.basename(raw_file)}"
                file_size_kb = os.path.getsize(raw_file) / 1024.0
                logger.info(f"Uploading True Bronze file to s3://{self.bucket_name}/{key} ({file_size_kb:.1f} KB)")
                s3_client.upload_file(
                    Filename=raw_file, Bucket=self.bucket_name, Key=key, ExtraArgs={"ContentType": "application/gzip"}
                )
                uploaded.append(key)
            else:
                # Fallback: upload normalized .jsonl if raw .gz is not present
                if has_normalized:
                    key = f"{landing_prefix}/{os.path.basename(norm_file)}"
                    file_size_kb = os.path.getsize(norm_file) / 1024.0
                    logger.info(
                        f"Uploading normalized session to s3://{self.bucket_name}/{key} ({file_size_kb:.1f} KB)"
                    )
                    s3_client.upload_file(
                        Filename=norm_file,
                        Bucket=self.bucket_name,
                        Key=key,
                        ExtraArgs={"ContentType": "application/x-ndjson"},
                    )
                    uploaded.append(key)

            # 2. Upload match summary metadata
            if has_summary:
                sum_key = f"{landing_prefix}/{os.path.basename(summary_file)}"
                logger.info(f"Uploading session summary to s3://{self.bucket_name}/{sum_key}")
                s3_client.upload_file(
                    Filename=summary_file,
                    Bucket=self.bucket_name,
                    Key=sum_key,
                    ExtraArgs={"ContentType": "application/json"},
                )
                uploaded.append(sum_key)

            logger.info(f"Successfully uploaded session {session_id} to S3 landing: {uploaded}")
            return {
                "status": "success",
                "session_id": session_id,
                "bucket": self.bucket_name,
                "uploaded_keys": uploaded,
                "timestamp": datetime.utcnow().isoformat(),
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
    parser.add_argument("--session", type=str, help="Session UUID to upload to landing/")
    parser.add_argument("--date", type=str, help="Target batch date (YYYY-MM-DD)")
    args = parser.parse_args()

    archiver = S3Archiver()

    if args.test:
        res = archiver.test_s3_connection()
        print(json.dumps(res, indent=2))
    elif args.session:
        res = archiver.upload_session_to_landing(args.session, game_end_date=args.date)
        print(json.dumps(res, indent=2))
    else:
        print("Usage: python -m services.archiver.s3_archiver --test | --session <uuid> [--date YYYY-MM-DD]")
