ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION=3.11

COPY orchestration/requirements.txt /tmp/rift-pulse-airflow-requirements.txt
COPY requirements-dbt.txt /tmp/rift-pulse-dbt-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/rift-pulse-airflow-requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/python -m pip install --no-cache-dir --upgrade pip \
    && /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/rift-pulse-dbt-requirements.txt
USER airflow
