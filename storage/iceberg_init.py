"""Initialize Iceberg namespaces and streaming table contracts through Trino.

This script is intentionally dependency-light and uses Trino's HTTP statement
API directly. It can run from the host against ``localhost:8080`` or inside the
Docker network against ``trino:8080``.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


LOGGER = logging.getLogger("iceberg-init")


@dataclass(frozen=True)
class InitConfig:
    """Configuration for Trino-backed Iceberg initialization."""

    trino_statement_url: str = "http://localhost:8080/v1/statement"
    trino_user: str = "admin"
    trino_catalog: str = "iceberg"
    retry_attempts: int = 30
    retry_delay_seconds: float = 2.0
    ddl_retry_attempts: int = 5
    ddl_retry_delay_seconds: float = 2.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "InitConfig":
        return cls(
            trino_statement_url=os.getenv(
                "TRINO_STATEMENT_URL", cls.trino_statement_url
            ),
            trino_user=os.getenv("TRINO_USER", cls.trino_user),
            trino_catalog=os.getenv("TRINO_CATALOG", cls.trino_catalog),
            retry_attempts=max(1, _int_env("ICEBERG_INIT_RETRY_ATTEMPTS", cls.retry_attempts)),
            retry_delay_seconds=max(
                0.1,
                _float_env("ICEBERG_INIT_RETRY_DELAY_SECONDS", cls.retry_delay_seconds),
            ),
            ddl_retry_attempts=max(
                1, _int_env("ICEBERG_INIT_DDL_RETRY_ATTEMPTS", cls.ddl_retry_attempts)
            ),
            ddl_retry_delay_seconds=max(
                0.1,
                _float_env(
                    "ICEBERG_INIT_DDL_RETRY_DELAY_SECONDS",
                    cls.ddl_retry_delay_seconds,
                ),
            ),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


class TrinoClient:
    """Small Trino HTTP client for DDL bootstrap statements."""

    def __init__(self, config: InitConfig) -> None:
        self.config = config

    def execute(self, sql: str) -> dict[str, Any]:
        """Execute one Trino SQL statement and return the final response."""

        LOGGER.debug("Executing SQL: %s", sql)
        response = self._request(self.config.trino_statement_url, data=sql.encode("utf-8"))
        response = self._raise_for_trino_error(response, sql)

        while response.get("nextUri"):
            time.sleep(0.1)
            response = self._request(response["nextUri"])
            response = self._raise_for_trino_error(response, sql)

        return response

    def execute_with_retry(self, sql: str) -> dict[str, Any]:
        """Execute one DDL statement with retries for local catalog races."""

        last_error: Exception | None = None
        for attempt in range(1, self.config.ddl_retry_attempts + 1):
            try:
                return self.execute(sql)
            except Exception as exc:
                last_error = exc
                LOGGER.warning(
                    "DDL failed attempt=%s/%s error=%s",
                    attempt,
                    self.config.ddl_retry_attempts,
                    exc,
                )
                if attempt < self.config.ddl_retry_attempts:
                    time.sleep(self.config.ddl_retry_delay_seconds)

        raise RuntimeError("DDL failed after retry attempts") from last_error

    def wait_until_ready(self) -> None:
        """Retry until Trino accepts statements and the Iceberg catalog is visible."""

        last_error: Exception | None = None
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                self.execute(f"SHOW SCHEMAS FROM {self.config.trino_catalog}")
                LOGGER.info("Trino and Iceberg catalog are ready")
                return
            except Exception as exc:
                last_error = exc
                LOGGER.warning(
                    "Waiting for Trino/Iceberg catalog attempt=%s/%s error=%s",
                    attempt,
                    self.config.retry_attempts,
                    exc,
                )
                time.sleep(self.config.retry_delay_seconds)

        raise RuntimeError("Trino/Iceberg catalog did not become ready") from last_error

    def _request(self, url: str, data: bytes | None = None) -> dict[str, Any]:
        request = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "text/plain",
                "X-Trino-User": self.config.trino_user,
                "X-Trino-Catalog": self.config.trino_catalog,
                "X-Trino-Source": "fintech-iceberg-init",
            },
            method="POST" if data is not None else "GET",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Trino HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Unable to reach Trino at {url}: {exc}") from exc

    @staticmethod
    def _raise_for_trino_error(response: dict[str, Any], sql: str) -> dict[str, Any]:
        if "error" not in response:
            return response

        error = response["error"]
        message = error.get("message", "unknown Trino error")
        error_name = error.get("errorName", "UNKNOWN")
        raise RuntimeError(f"{error_name}: {message}; sql={sql}")


def main() -> None:
    config = InitConfig.from_env()
    configure_logging(config.log_level)

    LOGGER.info(
        "Initializing Iceberg catalog=%s via Trino=%s user=%s",
        config.trino_catalog,
        config.trino_statement_url,
        config.trino_user,
    )

    client = TrinoClient(config)
    client.wait_until_ready()

    for statement in build_init_statements(config.trino_catalog):
        client.execute_with_retry(statement)
        LOGGER.info("Applied: %s", compact_sql(statement))

    LOGGER.info("Iceberg namespaces and table contracts are initialized")


def build_init_statements(catalog: str) -> list[str]:
    """Return idempotent DDL for Bronze/Silver tables and Gold namespace.

    Gold tables are owned by dbt models. Pre-creating them here can race dbt's
    drop/create table materialization against the local SQLite-backed Iceberg
    REST fixture.
    """

    catalog = sql_identifier(catalog)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold",
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.transactions (
            transaction_id varchar,
            account_id varchar,
            amount decimal(18, 2),
            currency varchar,
            event_time_epoch_us bigint,
            device_id varchar,
            location varchar,
            is_flagged_suspicious boolean,
            event_time timestamp(3),
            ingest_time timestamp(3),
            year integer,
            month integer,
            day integer,
            hour integer
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            compression_codec = 'ZSTD',
            partitioning = ARRAY['year', 'month', 'day', 'hour'],
            sorted_by = ARRAY['event_time', 'transaction_id'],
            max_commit_retry = 10,
            delete_after_commit_enabled = true,
            max_previous_versions = 20,
            object_store_layout_enabled = true
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.transactions (
            transaction_id varchar,
            account_id varchar,
            amount decimal(18, 2),
            currency varchar,
            event_time_epoch_us bigint,
            device_id varchar,
            location varchar,
            is_flagged_suspicious boolean,
            event_time timestamp(3),
            dedup_window_start timestamp(3),
            dedup_window_end timestamp(3),
            ingest_time timestamp(3),
            year integer,
            month integer,
            day integer,
            hour integer
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            compression_codec = 'ZSTD',
            partitioning = ARRAY['year', 'month', 'day', 'hour'],
            sorted_by = ARRAY['event_time', 'transaction_id'],
            max_commit_retry = 10,
            delete_after_commit_enabled = true,
            max_previous_versions = 20,
            object_store_layout_enabled = true
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.bronze.transactions_rejected (
            transaction_id varchar,
            account_id varchar,
            amount decimal(18, 2),
            currency varchar,
            event_time_epoch_us bigint,
            device_id varchar,
            location varchar,
            is_flagged_suspicious boolean,
            event_time timestamp(3),
            reject_reason varchar,
            rejected_at timestamp(3)
        )
        WITH (
            format = 'PARQUET',
            format_version = 2,
            compression_codec = 'ZSTD',
            sorted_by = ARRAY['rejected_at', 'reject_reason'],
            max_commit_retry = 10,
            delete_after_commit_enabled = true,
            max_previous_versions = 20,
            object_store_layout_enabled = true
        )
        """,
    ]


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def compact_sql(sql: str) -> str:
    return " ".join(sql.split())


def sql_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        LOGGER.warning("Invalid integer for %s=%r; using default %s", name, value, default)
        return default


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        LOGGER.warning("Invalid float for %s=%r; using default %s", name, value, default)
        return default


if __name__ == "__main__":
    main()
