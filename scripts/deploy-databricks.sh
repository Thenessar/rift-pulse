#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_dir}"

for command_name in databricks python3; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command is missing: ${command_name}" >&2
    exit 1
  fi
done

: "${AWS_S3_BUCKET:?Set AWS_S3_BUCKET to the Rift Pulse data-lake bucket name}"
: "${DATABRICKS_CATALOG:?Set DATABRICKS_CATALOG to an existing Unity Catalog catalog}"

target="prod"
bronze_schema="${DATABRICKS_BRONZE_SCHEMA:-bronze}"
bundle_args=(
  --target "${target}"
  --var "s3_bucket=${AWS_S3_BUCKET}"
  --var "catalog=${DATABRICKS_CATALOG}"
  --var "bronze_schema=${bronze_schema}"
)

databricks bundle validate "${bundle_args[@]}"
databricks bundle deploy "${bundle_args[@]}"

summary="$(databricks bundle summary "${bundle_args[@]}" --output json)"
job_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["resources"]["jobs"]["landing_to_bronze"]["id"])' <<<"${summary}")"

echo "Databricks job deployed successfully."
echo "Set DATABRICKS_JOB_ID=${job_id} in orchestration/.env"
echo "Optional smoke run: databricks bundle run ${bundle_args[*]} landing_to_bronze"
