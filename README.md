# Rift Pulse

Rift Pulse records League of Legends Live Client telemetry, calculates live match metrics and win probability, serves a broadcast HUD, and uploads completed match sessions to an S3 landing zone.

## Data flow

1. The engine polls the local LoL Live Client API.
2. During a match it writes normalized JSONL and versioned raw observation envelopes to compressed JSONL in `data/matches`.
3. When the match ends, the raw file is validated and uploaded under a content-addressed S3 key. A recording manifest is uploaded last as its commit marker.
4. Airflow triggers the Databricks job at 01:00 Europe/Warsaw.
5. `lakehouse/notebooks/01_landing_to_bronze.py` uses Auto Loader `AvailableNow` to preserve observation and manifest JSON as text in separate Bronze Delta tables.
6. `lakehouse/notebooks/02_bronze_to_silver.py` parses envelope v1 and legacy records, validates and deduplicates observations, and rebuilds the small Silver MVP tables.
7. After Silver succeeds, Airflow runs dbt Core against a Databricks SQL Warehouse. Staging views and tested Gold tables are written to `rift_pulse.gold`.

The Silver MVP exposes `matches`, `observations`, `match_participants`, `match_participant_items`, and `quarantine_observations`. dbt publishes `player_match_builds`, `champion_balance`, and `item_balance`. A model prediction is never treated as a match result. Win rate uses only sessions where a Live Client `GameEnd` result was mapped exactly to the active player's team; all other outcomes remain `NULL`.

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

To validate and upload all locally retained raw sessions using idempotent, content-addressed landing keys:

```powershell
python -m services.archiver.s3_archiver --all
```

## Tests

```powershell
ruff check .
ruff format --check .
python -m unittest discover -s tests -p "test_*.py"
```

Run `ruff check . --fix` to apply safe lint fixes and `ruff format .` to format Python files. The same checks run automatically in CI for every push and pull request.

Tests use `tests/fixtures/sample_live_data.json` and mock post-match S3 uploads. They do not require a running League client or write to AWS.

## Airflow and Databricks

The Databricks job is defined in `databricks.yml` and `resources/landing_to_bronze.job.yml`. Airflow triggers and monitors that job, then runs the pinned dbt adapter inside its container. Databricks owns S3 access and the SQL Warehouse executes Gold transformations.

See `docs/airflow-ubuntu.md` for the complete Databricks deployment, Unity Catalog prerequisites, Ubuntu setup, smoke test, and recovery commands.

## Model

The runtime model is stored at `services/engine/models/model.onnx`. To regenerate the baseline model:

```powershell
python services/engine/models/create_baseline_model.py
```
