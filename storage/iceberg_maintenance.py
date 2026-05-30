"""Run Apache Iceberg table maintenance through Trino.

The Docker Compose file already exposes an ``iceberg-maintenance`` profile. This
script keeps that profile usable by compacting data files, optimizing manifests,
expiring old snapshots, and removing orphan files for configured Iceberg tables.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import time

try:
    from iceberg_init import InitConfig, TrinoClient, compact_sql
except ImportError:
    from storage.iceberg_init import InitConfig, TrinoClient, compact_sql


LOGGER = logging.getLogger("iceberg-maintenance")
SIZE_PATTERN = re.compile(r"^[1-9][0-9]*(B|KB|MB|GB|TB)$", re.IGNORECASE)
DURATION_PATTERN = re.compile(r"^[1-9][0-9]*(ms|s|m|h|d)$", re.IGNORECASE)


def main() -> None:
    args = parse_args()
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))

    loop = args.loop or (
        not args.once and _bool_env("ICEBERG_MAINTENANCE_LOOP", False)
    )
    interval_seconds = max(1, _int_env("ICEBERG_MAINTENANCE_INTERVAL_SECONDS", 21600))

    while True:
        run_once()
        if not loop:
            break
        LOGGER.info("Sleeping %s seconds before next maintenance cycle", interval_seconds)
        time.sleep(interval_seconds)


def run_once() -> None:
    config = InitConfig.from_env()
    client = TrinoClient(config)
    client.wait_until_ready()

    for table_name in parse_tables():
        run_table_maintenance(client, table_name)


def run_table_maintenance(client: TrinoClient, table_name: str) -> None:
    """Run safe maintenance procedures for one Iceberg table."""

    table_name = validate_table_name(table_name)
    file_size_threshold = validate_size_threshold(
        os.getenv("ICEBERG_MAINTENANCE_FILE_SIZE_THRESHOLD", "64MB")
    )
    retention_threshold = validate_duration_threshold(
        os.getenv("ICEBERG_MAINTENANCE_RETENTION_THRESHOLD", "7d")
    )
    retain_last = max(1, _int_env("ICEBERG_MAINTENANCE_RETAIN_LAST", 10))

    statements = [
        f"ALTER TABLE {table_name} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')",
        f"ALTER TABLE {table_name} EXECUTE optimize_manifests",
        (
            f"ALTER TABLE {table_name} EXECUTE expire_snapshots("
            f"retention_threshold => '{retention_threshold}', retain_last => {retain_last})"
        ),
        f"ALTER TABLE {table_name} EXECUTE remove_orphan_files(retention_threshold => '{retention_threshold}')",
    ]

    LOGGER.info("Starting Iceberg maintenance for %s", table_name)
    for statement in statements:
        client.execute_with_retry(statement)
        LOGGER.info("Applied: %s", compact_sql(statement))
    LOGGER.info("Finished Iceberg maintenance for %s", table_name)


def parse_tables() -> list[str]:
    raw_tables = os.getenv("ICEBERG_MAINTENANCE_TABLES", "iceberg.silver.transactions")
    tables = [table.strip() for table in raw_tables.split(",") if table.strip()]
    if not tables:
        raise ValueError("ICEBERG_MAINTENANCE_TABLES must include at least one table")
    return tables


def validate_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Table must be fully qualified as catalog.schema.table: {table_name!r}")
    for part in parts:
        if not part or not part.replace("_", "").isalnum() or part[0].isdigit():
            raise ValueError(f"Unsafe table identifier: {table_name!r}")
    return table_name


def validate_size_threshold(value: str) -> str:
    if not SIZE_PATTERN.fullmatch(value.strip()):
        raise ValueError(f"Unsafe or invalid file size threshold: {value!r}")
    return value.strip().upper()


def validate_duration_threshold(value: str) -> str:
    if not DURATION_PATTERN.fullmatch(value.strip()):
        raise ValueError(f"Unsafe or invalid duration threshold: {value!r}")
    return value.strip().lower()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Trino-driven Iceberg maintenance")
    parser.add_argument("--once", action="store_true", help="Run one maintenance cycle and exit")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        LOGGER.warning("Invalid integer for %s=%r; using default %s", name, value, default)
        return default


if __name__ == "__main__":
    main()
