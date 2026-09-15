#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
orchestration_dir="$(cd -- "${script_dir}/.." && pwd)"
cd "${orchestration_dir}"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "Docker Engine with the Compose plugin is required." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created orchestration/.env. Replace all placeholder values, then run this script again." >&2
  exit 1
fi

required_keys=(
  POSTGRES_PASSWORD
  AIRFLOW__CORE__FERNET_KEY
  AIRFLOW_WEBSERVER_SECRET_KEY
  AIRFLOW_ADMIN_USERNAME
  AIRFLOW_ADMIN_PASSWORD
  DATABRICKS_HOST
  DATABRICKS_HTTP_PATH
  DATABRICKS_CATALOG
  DATABRICKS_CLIENT_ID
  DATABRICKS_CLIENT_SECRET
  DATABRICKS_JOB_ID
)

for key in "${required_keys[@]}"; do
  value="$(sed -n "s/^${key}=//p" .env | tail -n 1)"
  if [[ -z "${value}" || "${value}" == *replace_with* || "${value}" == *your-workspace* ]]; then
    echo "Set ${key} in orchestration/.env before startup." >&2
    exit 1
  fi
done

if ! grep -Eq '^DATABRICKS_JOB_ID=[0-9]+$' .env; then
  echo "DATABRICKS_JOB_ID must be numeric." >&2
  exit 1
fi

mkdir -p logs dbt-artifacts
docker compose config --quiet
docker compose build
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps

echo "Airflow is listening on Ubuntu localhost: http://127.0.0.1:8080"
