"""Kafka Avro transactions to clean stream output.

The job consumes the Confluent-wire-format Avro stream produced by the
transaction simulator, decodes values through Schema Registry, deduplicates
transactions per event-time window, masks PII, and writes the clean stream to a
Flink print sink. The print sink is intentionally temporary until the Iceberg
sink is wired in the next phase.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


LOGGER = logging.getLogger("fintech.kafka-to-iceberg")


@dataclass(frozen=True)
class JobConfig:
    """Runtime configuration loaded from environment variables."""

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    source_topic: str = "financial.transactions"
    schema_registry_subject: str = "financial.transactions-value"
    consumer_group_id: str = "flink-kafka-to-iceberg-phase3"
    startup_mode: str = "earliest-offset"
    dedup_window_minutes: int = 10
    watermark_lateness_seconds: int = 20
    checkpoint_interval_ms: int = 30_000
    checkpoint_min_pause_ms: int = 10_000
    checkpoint_timeout_ms: int = 120_000
    checkpoint_dir: str = "file:///tmp/flink/checkpoints/kafka-to-iceberg"
    rocksdb_local_dir: str = "/tmp/flink/rocksdb/kafka-to-iceberg"
    parallelism: int = 2
    sink_parallelism: int = 1
    print_identifier: str = "clean_transactions"
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
            sink_parallelism=max(1, _int_env("FLINK_SINK_PARALLELISM", cls.sink_parallelism)),
            print_identifier=os.getenv("FLINK_PRINT_IDENTIFIER", cls.print_identifier),
            pipeline_jars=_normalize_pipeline_jars(
                os.getenv("FLINK_PIPELINE_JARS")
                or os.getenv("PYFLINK_PIPELINE_JARS")
                or os.getenv("FLINK_CONNECTOR_JARS")
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
        "Starting PyFlink job topic=%s kafka=%s schema_registry=%s group_id=%s "
        "window_minutes=%s watermark_lateness_seconds=%s parallelism=%s",
        config.source_topic,
        config.kafka_bootstrap_servers,
        config.schema_registry_url,
        config.consumer_group_id,
        config.dedup_window_minutes,
        config.watermark_lateness_seconds,
        config.parallelism,
    )

    env, table_env = build_environments(config)
    register_tables(table_env, config)
    submit_insert(table_env, config)


def build_environments(
    config: JobConfig,
) -> tuple[StreamExecutionEnvironment, StreamTableEnvironment]:
    """Create the Flink streaming and table environments."""

    flink_config = build_flink_configuration(config)
    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(
        config.checkpoint_interval_ms,
        CheckpointingMode.EXACTLY_ONCE,
    )

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_timeout(config.checkpoint_timeout_ms)
    checkpoint_config.set_min_pause_between_checkpoints(config.checkpoint_min_pause_ms)
    checkpoint_config.set_max_concurrent_checkpoints(1)

    settings = (
        EnvironmentSettings.new_instance()
        .in_streaming_mode()
        .with_configuration(flink_config)
        .build()
    )
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)
    table_env.get_config().set("table.exec.state.ttl", config.table_state_ttl)

    LOGGER.info(
        "Configured RocksDB state backend checkpoint_dir=%s rocksdb_local_dir=%s "
        "checkpoint_interval_ms=%s table_state_ttl=%s",
        config.checkpoint_dir,
        config.rocksdb_local_dir,
        config.checkpoint_interval_ms,
        config.table_state_ttl,
    )
    return env, table_env


def build_flink_configuration(config: JobConfig) -> Configuration:
    """Return production-oriented Flink configuration for local submission."""

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
    flink_config.set_string("state.backend.type", "rocksdb")
    flink_config.set_string("state.backend.incremental", "true")
    flink_config.set_string("state.backend.rocksdb.memory.managed", "true")
    flink_config.set_string("state.backend.rocksdb.localdir", config.rocksdb_local_dir)
    flink_config.set_string("state.checkpoints.dir", config.checkpoint_dir)
    flink_config.set_string("table.local-time-zone", "UTC")

    if config.pipeline_jars:
        flink_config.set_string("pipeline.jars", config.pipeline_jars)
        LOGGER.info("Using pipeline.jars=%s", config.pipeline_jars)

    return flink_config


def register_tables(table_env: StreamTableEnvironment, config: JobConfig) -> None:
    """Register Kafka Avro source, validation view, and print sink."""

    table_env.execute_sql(
        f"""
        CREATE TABLE financial_transactions_raw (
            transaction_id STRING,
            account_id STRING,
            amount DOUBLE,
            currency STRING,
            `timestamp` STRING,
            event_time_epoch_us BIGINT,
            device_id STRING,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            event_time AS TO_TIMESTAMP_LTZ(
                CAST(event_time_epoch_us / 1000 AS BIGINT),
                3
            ),
            WATERMARK FOR event_time AS
                event_time - INTERVAL '{config.watermark_lateness_seconds}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = {sql_literal(config.source_topic)},
            'properties.bootstrap.servers' = {sql_literal(config.kafka_bootstrap_servers)},
            'properties.group.id' = {sql_literal(config.consumer_group_id)},
            'scan.startup.mode' = {sql_literal(config.startup_mode)},
            'format' = 'avro-confluent',
            'avro-confluent.url' = {sql_literal(config.schema_registry_url)},
            'avro-confluent.subject' = {sql_literal(config.schema_registry_subject)}
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
        f"""
        CREATE TABLE clean_transactions_print (
            transaction_id STRING,
            account_id_masked STRING,
            device_id_masked STRING,
            amount DOUBLE,
            currency STRING,
            event_time_utc STRING,
            event_time_epoch_us BIGINT,
            location STRING,
            is_flagged_suspicious BOOLEAN,
            dedup_window_start_utc STRING,
            dedup_window_end_utc STRING
        ) WITH (
            'connector' = 'print',
            'print-identifier' = {sql_literal(config.print_identifier)},
            'sink.parallelism' = {sql_literal(str(config.sink_parallelism))}
        )
        """
    )

    LOGGER.info("Registered Kafka source, validation view, and print sink")


def submit_insert(table_env: StreamTableEnvironment, config: JobConfig) -> None:
    """Run windowed dedupe, PII masking, and print-sink insert."""

    # Flink SQL window deduplication purges per-window state after the watermark
    # closes each window, and RocksDB backs the managed state to avoid heap growth.
    insert_result = table_env.execute_sql(
        f"""
        INSERT INTO clean_transactions_print
        SELECT
            transaction_id,
            CONCAT('ACC-SHA256-', SUBSTRING(SHA2(COALESCE(account_id, ''), 256), 1, 16))
                AS account_id_masked,
            CONCAT('DEV-SHA256-', SUBSTRING(SHA2(COALESCE(device_id, ''), 256), 1, 16))
                AS device_id_masked,
            amount,
            currency,
            CAST(event_time AS STRING) AS event_time_utc,
            event_time_epoch_us,
            location,
            is_flagged_suspicious,
            CAST(window_start AS STRING) AS dedup_window_start_utc,
            CAST(window_end AS STRING) AS dedup_window_end_utc
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
        "Submitted streaming insert. Windowed output appears after each %s minute "
        "window closes plus %s seconds of allowed lateness.",
        config.dedup_window_minutes,
        config.watermark_lateness_seconds,
    )
    insert_result.wait()


def configure_logging(level: str) -> None:
    """Configure Python logging; Flink captures this in cluster logs."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def sql_literal(value: str) -> str:
    """Escape a Python string as a Flink SQL string literal."""

    return "'" + value.replace("'", "''") + "'"


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
