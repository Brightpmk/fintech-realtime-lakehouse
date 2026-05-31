"""Kafka transactions into Iceberg Bronze/Silver.

The job consumes the Confluent-wire-format Avro stream produced by the
transaction simulator, decodes values through Schema Registry, and writes an
open lakehouse medallion layout on Iceberg:

* ``iceberg.bronze.transactions`` receives append-only validated decoded events.
* ``iceberg.bronze.transactions_rejected`` receives malformed decoded events.
* ``iceberg.silver.transactions`` receives event-time-window deduplicated and
  masked events partitioned by event time for efficient Trino/dbt reads.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyflink.common import Configuration
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment


LOGGER = logging.getLogger("fintech.kafka-to-iceberg")


@dataclass(frozen=True)
class JobConfig:
    """Runtime configuration loaded from environment variables."""

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    source_topic: str = "financial.transactions"
    schema_registry_subject: str = "financial.transactions-value"
    consumer_group_id: str = "flink-kafka-to-iceberg-phase4"
    startup_mode: str = "earliest-offset"
    iceberg_catalog_name: str = "iceberg"
    iceberg_rest_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = "us-east-1"
    pii_hash_salt: str | None = None
    dedup_window_minutes: int = 10
    watermark_lateness_seconds: int = 20
    checkpoint_interval_ms: int = 30_000
    checkpoint_min_pause_ms: int = 10_000
    checkpoint_timeout_ms: int = 120_000
    checkpoint_dir: str = "s3://warehouse/flink-checkpoints/kafka-to-iceberg"
    rocksdb_local_dir: str = "/tmp/flink/rocksdb/kafka-to-iceberg"
    parallelism: int = 2
    pipeline_jars: str | None = None
    table_state_ttl: str = "12 min"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "JobConfig":
        """Create a config object from env vars with local-safe defaults."""

        source_topic = os.getenv("KAFKA_TOPIC", cls.source_topic)
        dedup_window_minutes = max(
            1, _int_env("DEDUP_WINDOW_MINUTES", cls.dedup_window_minutes)
        )
        watermark_lateness_seconds = max(
            0,
            _int_env("WATERMARK_LATENESS_SECONDS", cls.watermark_lateness_seconds),
        )
        default_state_ttl_minutes = dedup_window_minutes + (
            watermark_lateness_seconds // 60
        ) + 2

        s3_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        s3_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        pii_hash_salt = os.getenv("PII_HASH_SALT")

        if not s3_access_key_id:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")
        if not s3_secret_access_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")
        if not pii_hash_salt:
            raise ValueError("PII_HASH_SALT environment variable is required")

        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap_servers
            ),
            schema_registry_url=os.getenv(
                "SCHEMA_REGISTRY_URL", cls.schema_registry_url
            ),
            source_topic=source_topic,
            schema_registry_subject=os.getenv(
                "SCHEMA_REGISTRY_SUBJECT", f"{source_topic}-value"
            ),
            consumer_group_id=os.getenv("FLINK_CONSUMER_GROUP_ID", cls.consumer_group_id),
            startup_mode=os.getenv("FLINK_STARTUP_MODE", cls.startup_mode),
            iceberg_catalog_name=os.getenv(
                "ICEBERG_CATALOG_NAME", cls.iceberg_catalog_name
            ),
            iceberg_rest_uri=os.getenv("ICEBERG_REST_URI", cls.iceberg_rest_uri),
            iceberg_warehouse=os.getenv("ICEBERG_WAREHOUSE", cls.iceberg_warehouse),
            s3_endpoint=os.getenv(
                "S3_ENDPOINT",
                os.getenv("MINIO_ENDPOINT", cls.s3_endpoint),
            ),
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_region=os.getenv("AWS_REGION", cls.s3_region),
            pii_hash_salt=pii_hash_salt,
            dedup_window_minutes=dedup_window_minutes,
            watermark_lateness_seconds=watermark_lateness_seconds,
            checkpoint_interval_ms=max(
                1_000, _int_env("FLINK_CHECKPOINT_INTERVAL_MS", cls.checkpoint_interval_ms)
            ),
            checkpoint_min_pause_ms=max(
                0, _int_env("FLINK_CHECKPOINT_MIN_PAUSE_MS", cls.checkpoint_min_pause_ms)
            ),
            checkpoint_timeout_ms=max(
                10_000, _int_env("FLINK_CHECKPOINT_TIMEOUT_MS", cls.checkpoint_timeout_ms)
            ),
            checkpoint_dir=os.getenv("FLINK_CHECKPOINT_DIR", cls.checkpoint_dir),
            rocksdb_local_dir=os.getenv("FLINK_ROCKSDB_LOCAL_DIR", cls.rocksdb_local_dir),
            parallelism=max(1, _int_env("FLINK_PARALLELISM", cls.parallelism)),
            pipeline_jars=(
                _normalize_pipeline_jars(
                    os.getenv("FLINK_PIPELINE_JARS")
                    or os.getenv("PYFLINK_PIPELINE_JARS")
                    or os.getenv("FLINK_CONNECTOR_JARS")
                )
                or _discover_pipeline_jars(
                    Path(os.getenv("FLINK_JARS_DIR", "/opt/flink/usrlib"))
                )
            ),
            table_state_ttl=os.getenv(
                "FLINK_TABLE_STATE_TTL", f"{default_state_ttl_minutes} min"
            ),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


def main() -> None:
    """Build and submit the streaming job."""

    config = JobConfig.from_env()
    configure_logging(config.log_level)

    LOGGER.info(
        "Starting PyFlink Iceberg job topic=%s kafka=%s schema_registry=%s "
        "iceberg_rest=%s warehouse=%s s3_endpoint=%s group_id=%s",
        config.source_topic,
        config.kafka_bootstrap_servers,
        config.schema_registry_url,
        config.iceberg_rest_uri,
        config.iceberg_warehouse,
        config.s3_endpoint,
        config.consumer_group_id,
    )

    _, table_env = build_environments(config)
    register_kafka_source(table_env, config)
    register_iceberg_catalog_and_tables(table_env, config)
    submit_medallion_inserts(table_env, config)


def build_environments(
    config: JobConfig,
) -> tuple["StreamExecutionEnvironment", "StreamTableEnvironment"]:
    """Create the Flink streaming and table environments."""

    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is required to run the streaming job. Install apache-flink "
            "or submit this file with a Flink Python runtime."
        ) from exc

    flink_config = build_flink_configuration(config)
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(config.parallelism)

    settings = (
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(flink_config)
        .build()
    )
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    table_env.get_config().set("table.exec.state.ttl", config.table_state_ttl)
    table_env.get_config().set("table.local-time-zone", "UTC")
    table_env.get_config().set("table.optimizer.reuse-source-enabled", "true")
    table_env.get_config().set("table.optimizer.reuse-sub-plan-enabled", "true")

    LOGGER.info(
        "Configured RocksDB state backend checkpoint_dir=%s rocksdb_local_dir=%s "
        "checkpoint_interval_ms=%s table_state_ttl=%s",
        config.checkpoint_dir,
        config.rocksdb_local_dir,
        config.checkpoint_interval_ms,
        config.table_state_ttl,
    )
    return env, table_env


def build_flink_configuration(config: JobConfig) -> "Configuration":
    """Return production-oriented Flink configuration for local submission."""

    try:
        from pyflink.common import Configuration
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is required to build the Flink runtime configuration."
        ) from exc

    flink_config = Configuration()
    flink_config.set_string("parallelism.default", str(config.parallelism))
    flink_config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    flink_config.set_string(
        "execution.checkpointing.interval", f"{config.checkpoint_interval_ms} ms"
    )
    flink_config.set_string(
        "execution.checkpointing.min-pause", f"{config.checkpoint_min_pause_ms} ms"
    )
    flink_config.set_string(
        "execution.checkpointing.timeout", f"{config.checkpoint_timeout_ms} ms"
    )
    flink_config.set_string("execution.checkpointing.max-concurrent-checkpoints", "1")
    flink_config.set_string("state.backend.type", "rocksdb")
    flink_config.set_string("state.backend.incremental", "true")
    flink_config.set_string("state.backend.rocksdb.memory.managed", "true")
    flink_config.set_string("state.backend.rocksdb.localdir", config.rocksdb_local_dir)
    flink_config.set_string("state.checkpoints.dir", config.checkpoint_dir)
    flink_config.set_string("table.local-time-zone", "UTC")
    flink_config.set_string("s3.endpoint", config.s3_endpoint)
    flink_config.set_string("s3.path.style.access", "true")
    flink_config.set_string("s3.region", config.s3_region)
    flink_config.set_string("s3.access-key", config.s3_access_key_id)
    flink_config.set_string("s3.secret-key", config.s3_secret_access_key)

    if config.pipeline_jars:
        flink_config.set_string("pipeline.jars", config.pipeline_jars)
        LOGGER.info("Using pipeline.jars=%s", config.pipeline_jars)

    return flink_config


def register_kafka_source(
    table_env: "StreamTableEnvironment",
    config: JobConfig,
) -> None:
    """Register the Schema Registry backed Kafka source and validity view."""

    table_env.execute_sql(
        f"""
        CREATE TABLE financial_transactions_raw (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time_epoch_us BIGINT,
            device_id STRING,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            event_time AS (
                CASE
                    WHEN event_time_epoch_us IS NULL THEN NULL
                    ELSE TO_TIMESTAMP_LTZ(
                        CAST(FLOOR(event_time_epoch_us / 1000) AS BIGINT),
                        3
                    )
                END
            ),
            WATERMARK FOR event_time AS
                event_time - INTERVAL '{config.watermark_lateness_seconds}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_literal(config.source_topic)},
            'properties.bootstrap.servers' = {sql_literal(config.kafka_bootstrap_servers)},
            'properties.group.id' = {sql_literal(config.consumer_group_id)},
            'scan.startup.mode' = {sql_literal(config.startup_mode)},
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = {sql_literal(config.schema_registry_url)},
            'value.avro-confluent.subject' = {sql_literal(config.schema_registry_subject)}
        )
        """
    )

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW financial_transactions_valid AS
        SELECT *
        FROM financial_transactions_raw
        WHERE transaction_id IS NOT NULL
          AND event_time_epoch_us IS NOT NULL
          AND event_time IS NOT NULL
        """
    )

    table_env.execute_sql(
        """
        CREATE TEMPORARY VIEW financial_transactions_invalid AS
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            event_time_epoch_us,
            device_id,
            location,
            is_flagged_suspicious,
            event_time,
            CASE
                WHEN transaction_id IS NULL THEN 'MISSING_TRANSACTION_ID'
                WHEN event_time_epoch_us IS NULL THEN 'MISSING_EVENT_TIME_EPOCH_US'
                WHEN event_time IS NULL THEN 'INVALID_EVENT_TIME'
                ELSE 'UNKNOWN_CONTRACT_VIOLATION'
            END AS reject_reason
        FROM financial_transactions_raw
        WHERE transaction_id IS NULL
           OR event_time_epoch_us IS NULL
           OR event_time IS NULL
        """
    )

    LOGGER.info("Registered Schema Registry backed Kafka source")


def register_iceberg_catalog_and_tables(
    table_env: "StreamTableEnvironment",
    config: JobConfig,
) -> None:
    """Register the Iceberg REST catalog and idempotent Bronze/Silver tables."""

    catalog_name = sql_identifier(config.iceberg_catalog_name)
    table_env.execute_sql(
        f"""
        CREATE CATALOG {catalog_name} WITH (
            'type' = 'iceberg',
            'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
            'uri' = {sql_literal(config.iceberg_rest_uri)},
            'warehouse' = {sql_literal(config.iceberg_warehouse)},
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = {sql_literal(config.s3_endpoint)},
            's3.access-key-id' = {sql_literal(config.s3_access_key_id)},
            's3.secret-access-key' = {sql_literal(config.s3_secret_access_key)},
            's3.path-style-access' = 'true',
            's3.region' = {sql_literal(config.s3_region)}
        )
        """
    )

    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.bronze")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.silver")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.gold")

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.transactions (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time_epoch_us BIGINT NOT NULL,
            device_id STRING,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            event_time TIMESTAMP(6),
            ingest_time TIMESTAMP(6),
            `year` INT,
            `month` INT,
            `day` INT,
            `hour` INT
        )
        PARTITIONED BY (`year`, `month`, `day`, `hour`)
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '134217728',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '20'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.silver.transactions (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time_epoch_us BIGINT NOT NULL,
            device_id STRING,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            event_time TIMESTAMP(6),
            dedup_window_start TIMESTAMP(6),
            dedup_window_end TIMESTAMP(6),
            ingest_time TIMESTAMP(6),
            `year` INT,
            `month` INT,
            `day` INT,
            `hour` INT
        )
        PARTITIONED BY (`year`, `month`, `day`, `hour`)
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '134217728',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '20'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.bronze.transactions_rejected (
            transaction_id STRING,
            account_id STRING,
            amount DECIMAL(18, 2),
            currency STRING,
            event_time_epoch_us BIGINT,
            device_id STRING,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            event_time TIMESTAMP(6),
            reject_reason STRING,
            rejected_at TIMESTAMP(6)
        )
        WITH (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '134217728',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '20'
        )
        """
    )

    LOGGER.info("Registered Iceberg catalog and Bronze/Silver/rejected tables")


def submit_medallion_inserts(
    table_env: "StreamTableEnvironment",
    config: JobConfig,
) -> None:
    """Run concurrent Bronze validated/rejected and Silver deduplicated writes."""

    catalog_name = sql_identifier(config.iceberg_catalog_name)
    statement_set = table_env.create_statement_set()

    statement_set.add_insert_sql(
        f"""
        INSERT INTO {catalog_name}.bronze.transactions
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            event_time_epoch_us,
            device_id,
            location,
            is_flagged_suspicious,
            CASE 
                WHEN event_time IS NULL THEN NULL 
                ELSE CAST(event_time AS TIMESTAMP(6)) 
            END AS event_time,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS ingest_time,
            CAST(EXTRACT(YEAR FROM event_time) AS INT) AS `year`,
            CAST(EXTRACT(MONTH FROM event_time) AS INT) AS `month`,
            CAST(EXTRACT(DAY FROM event_time) AS INT) AS `day`,
            CAST(EXTRACT(HOUR FROM event_time) AS INT) AS `hour`
        FROM financial_transactions_valid
        """
    )

    statement_set.add_insert_sql(
        f"""
        INSERT INTO {catalog_name}.bronze.transactions_rejected
        SELECT
            transaction_id,
            account_id,
            amount,
            currency,
            event_time_epoch_us,
            device_id,
            location,
            is_flagged_suspicious,
            CAST(event_time AS TIMESTAMP(6)) AS event_time,
            reject_reason,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS rejected_at
        FROM financial_transactions_invalid
        """
    )

    # Flink SQL window deduplication purges per-window state after the watermark
    # closes each window, and RocksDB backs the managed state to avoid heap growth.
    statement_set.add_insert_sql(
        f"""
        INSERT INTO {catalog_name}.silver.transactions
        SELECT
            transaction_id,
            CONCAT(
                'ACC-SHA256-',
                SUBSTRING(
                    SHA2(
                        CONCAT({sql_literal(config.pii_hash_salt)}, ':', COALESCE(account_id, '')),
                        256
                    ),
                    1,
                    16
                )
            )
                AS account_id,
            amount,
            currency,
            event_time_epoch_us,
            CONCAT(
                'DEV-SHA256-',
                SUBSTRING(
                    SHA2(
                        CONCAT({sql_literal(config.pii_hash_salt)}, ':', COALESCE(device_id, '')),
                        256
                    ),
                    1,
                    16
                )
            )
                AS device_id,
            location,
            is_flagged_suspicious,
            CAST(event_time AS TIMESTAMP(6)) AS event_time,
            CAST(window_start AS TIMESTAMP(6)) AS dedup_window_start,
            CAST(window_end AS TIMESTAMP(6)) AS dedup_window_end,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS ingest_time,
            CAST(EXTRACT(YEAR FROM event_time) AS INT) AS `year`,
            CAST(EXTRACT(MONTH FROM event_time) AS INT) AS `month`,
            CAST(EXTRACT(DAY FROM event_time) AS INT) AS `day`,
            CAST(EXTRACT(HOUR FROM event_time) AS INT) AS `hour`
        FROM (
            SELECT
                transaction_id,
                account_id,
                device_id,
                amount,
                currency,
                event_time,
                event_time_epoch_us,
                location,
                is_flagged_suspicious,
                window_start,
                window_end,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end, transaction_id
                    ORDER BY event_time ASC
                ) AS rownum
            FROM TABLE(
                TUMBLE(
                    TABLE financial_transactions_valid,
                    DESCRIPTOR(event_time),
                    INTERVAL '{config.dedup_window_minutes}' MINUTES
                )
            )
        )
        WHERE rownum = 1
        """
    )

    LOGGER.info(
        "Submitted Bronze/Silver Iceberg inserts. Silver rows are emitted after "
        "each %s minute event-time window closes plus %s seconds of allowed lateness.",
        config.dedup_window_minutes,
        config.watermark_lateness_seconds,
    )
    statement_set.execute().wait()


def configure_logging(level: str) -> None:
    """Configure Python logging; Flink captures this in cluster logs."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def sql_literal(value: str) -> str:
    """Escape a Python string as a Flink SQL string literal."""

    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    """Validate a simple Flink SQL identifier."""

    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _normalize_pipeline_jars(raw_value: str | None) -> str | None:
    if not raw_value:
        return None

    normalized: list[str] = []
    for value in raw_value.replace(",", ";").split(";"):
        jar = value.strip()
        if not jar:
            continue
        if "://" in jar:
            normalized.append(jar)
        else:
            normalized.append(Path(jar).expanduser().resolve().as_uri())

    return ";".join(normalized) if normalized else None


def _discover_pipeline_jars(jars_dir: Path) -> str | None:
    if not jars_dir.exists() or not jars_dir.is_dir():
        return None

    jar_uris = [jar.resolve().as_uri() for jar in sorted(jars_dir.glob("*.jar"))]
    return ";".join(jar_uris) if jar_uris else None


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
