# Rift Pulse

Rift Pulse records League of Legends Live Client telemetry, calculates live match metrics and win probability, serves a broadcast HUD, and uploads completed match sessions to an S3 landing zone.

## Data flow

1. The engine polls the local LoL Live Client API.
2. During a match it writes normalized JSONL and raw compressed JSONL to `data/matches`.
3. When the match ends, the raw session and summary are uploaded to `s3://<bucket>/landing/date=YYYY-MM-DD/`.
4. Airflow triggers the Databricks job once per day.
5. The canonical notebook at `lakehouse/notebooks/01_landing_to_bronze.py` ingests landing data into the `bronze_matches` Delta table and runs `OPTIMIZE`.

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

Copy `orchestration/.env.example` to `orchestration/.env`, configure AWS and Databricks credentials, and start Airflow from the orchestration directory:

```powershell
docker compose up -d
```

Only `daily_lakehouse_ingest_dag` triggers ingestion. The Databricks job identified by `DATABRICKS_JOB_ID` should execute `lakehouse/notebooks/01_landing_to_bronze.py`.

## Model

The runtime model is stored at `services/engine/models/model.onnx`. To regenerate the baseline model:

```powershell
python services/engine/models/create_baseline_model.py
```
