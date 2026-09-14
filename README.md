# Rift Pulse

Rift Pulse records League of Legends Live Client telemetry, calculates live match metrics and win probability, serves a broadcast HUD, and uploads completed match sessions to an S3 landing zone.

## Data flow

1. The engine polls the local LoL Live Client API.
2. During a match it writes normalized JSONL and raw compressed JSONL to `data/matches`.
3. When the match ends, the raw session and summary are uploaded to `s3://<bucket>/landing/date=YYYY-MM-DD/`.
4. Airflow triggers the Databricks job at 01:00 Europe/Warsaw.
5. The canonical notebook at `lakehouse/notebooks/01_landing_to_bronze.py` uses Auto Loader `AvailableNow` to ingest raw gzip files into a Delta table backed by Parquet under `bronze/match_snapshots`.

## Local setup

Create a virtual environment, install the dependencies, and copy the environment template:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

Fill in the AWS settings in `.env`, then start the API and HUD:

```powershell
python -m services.engine.src.main
```

The application is available at `http://localhost:8000`. The League client endpoint defaults to `https://127.0.0.1:2999/liveclientdata/allgamedata`.

## Tests

```powershell
ruff check .
ruff format --check .
python -m unittest discover -s tests -p "test_*.py"
```

Run `ruff check . --fix` to apply safe lint fixes and `ruff format .` to format Python files. The same checks run automatically in CI for every push and pull request.

Tests use `tests/fixtures/sample_live_data.json` and mock post-match S3 uploads. They do not require a running League client or write to AWS.

## Airflow and Databricks

The Databricks job is defined in `databricks.yml` and `resources/landing_to_bronze.job.yml`. Airflow only triggers and monitors that job; Databricks owns S3 access and data processing.

See `docs/airflow-ubuntu.md` for the complete Databricks deployment, Unity Catalog prerequisites, Ubuntu setup, smoke test, and recovery commands.

## Model

The runtime model is stored at `services/engine/models/model.onnx`. To regenerate the baseline model:

```powershell
python services/engine/models/create_baseline_model.py
```
