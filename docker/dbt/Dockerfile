FROM python:3.12-slim

# Install uv inside container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN groupadd -r dbt && useradd -r -g dbt -m dbt

WORKDIR /workspace

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies using uv sync
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

USER dbt

# Place the virtual environment's bin on the PATH
ENV PATH="/workspace/.venv/bin:$PATH"
