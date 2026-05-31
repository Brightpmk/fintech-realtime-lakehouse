FROM python:3.12-slim

RUN groupadd -r dbt && useradd -r -g dbt -m dbt

WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /workspace/requirements.txt
COPY dbt_lakehouse/requirements.txt /workspace/dbt_lakehouse/requirements.txt

RUN pip install --no-cache-dir \
    -r /workspace/requirements.txt \
    -r /workspace/dbt_lakehouse/requirements.txt

USER dbt
