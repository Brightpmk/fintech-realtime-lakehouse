"""Kafka transactions into Iceberg Bronze/Silver."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from config import JobConfig, _normalize_pipeline_jars, _discover_pipeline_jars

if TYPE_CHECKING:
    from pyflink.common import Configuration
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

LOGGER = logging.getLogger("fintech.kafka-to-iceberg")


def main() -> None:
    config = JobConfig.from_env()
    configure_logging(config.log_level)
    _, table_env = build_environments(config)
    register_kafka_source(table_env, config)
    register_iceberg_catalog_and_tables(table_env, config)
    submit_medallion_inserts(table_env, config)


def build_environments(config: JobConfig) -> tuple["StreamExecutionEnvironment", "StreamTableEnvironment"]:
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    except ImportError as exc: raise RuntimeError("PyFlink required") from exc
    fc = build_flink_configuration(config)
    env = StreamExecutionEnvironment.get_execution_environment(fc)
    env.set_parallelism(config.parallelism)
    te = StreamTableEnvironment.create(env, EnvironmentSettings.new_instance().in_streaming_mode().with_configuration(fc).build())
    for k, v in [("table.exec.state.ttl", config.table_state_ttl), ("table.exec.source.idle-timeout", config.source_idle_timeout), ("table.local-time-zone", "UTC"), ("table.optimizer.reuse-source-enabled", "true"), ("table.optimizer.reuse-sub-plan-enabled", "true")]:
        te.get_config().set(k, v)
    return env, te


def build_flink_configuration(config: JobConfig) -> "Configuration":
    try:
        from pyflink.common import Configuration
    except ImportError as exc: raise RuntimeError("PyFlink required") from exc
    fc = Configuration()
    props = {
        "parallelism.default": str(config.parallelism), "execution.checkpointing.mode": "EXACTLY_ONCE",
        "execution.checkpointing.interval": f"{config.checkpoint_interval_ms} ms",
        "execution.checkpointing.min-pause": f"{config.checkpoint_min_pause_ms} ms",
        "execution.checkpointing.timeout": f"{config.checkpoint_timeout_ms} ms",
        "execution.checkpointing.max-concurrent-checkpoints": "1", "state.backend.type": "rocksdb",
        "state.backend.incremental": "true", "state.backend.rocksdb.memory.managed": "true",
        "state.backend.rocksdb.localdir": config.rocksdb_local_dir, "state.checkpoints.dir": config.checkpoint_dir,
        "table.local-time-zone": "UTC", "s3.endpoint": config.s3_endpoint, "s3.path.style.access": "true",
        "s3.region": config.s3_region, "s3.access-key": config.s3_access_key_id, "s3.secret-key": config.s3_secret_access_key,
    }
    for k, v in props.items():
        if v is not None: fc.set_string(k, v)
    if config.pipeline_jars: fc.set_string("pipeline.jars", config.pipeline_jars)
    return fc


def register_kafka_source(table_env: "StreamTableEnvironment", config: JobConfig) -> None:
    table_env.execute_sql(f"""
        CREATE TABLE financial_transactions_raw (
            transaction_id STRING NOT NULL, account_id STRING NOT NULL, amount DECIMAL(18, 2), currency STRING,
            event_time_epoch_us BIGINT, device_id STRING, location STRING, is_flagged_suspicious BOOLEAN,
            event_time AS (
                CASE
                    WHEN event_time_epoch_us IS NULL THEN NULL
                    WHEN event_time_epoch_us < 1000000000000000 THEN NULL
                    WHEN event_time_epoch_us > 9999999999999999 THEN NULL
                    ELSE TO_TIMESTAMP_LTZ(
                        CAST(FLOOR(event_time_epoch_us / 1000) AS BIGINT),
                        3
                    )
                END
            ),
            WATERMARK FOR event_time AS event_time - INTERVAL '{config.watermark_lateness_seconds}' SECOND
        ) WITH (
            'connector' = 'kafka', 'topic' = {sql_literal(config.source_topic)},
            'properties.bootstrap.servers' = {sql_literal(config.kafka_bootstrap_servers)},
            'properties.group.id' = {sql_literal(config.consumer_group_id)},
            'scan.startup.mode' = {sql_literal(config.startup_mode)}, 'properties.auto.offset.reset' = 'earliest',
            'value.format' = 'avro-confluent', 'value.avro-confluent.url' = {sql_literal(config.schema_registry_url)},
            'value.avro-confluent.subject' = {sql_literal(config.schema_registry_subject)}
        )
    """)
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW financial_transactions_valid AS
        SELECT * FROM financial_transactions_raw
        WHERE transaction_id IS NOT NULL AND event_time_epoch_us IS NOT NULL AND event_time IS NOT NULL
    """)
    table_env.execute_sql("""
        CREATE TEMPORARY VIEW financial_transactions_invalid AS
        SELECT transaction_id, account_id, amount, currency, event_time_epoch_us, device_id, location, is_flagged_suspicious, event_time,
            CASE WHEN transaction_id IS NULL THEN 'MISSING_TRANSACTION_ID' WHEN event_time_epoch_us IS NULL THEN 'MISSING_EVENT_TIME_EPOCH_US' WHEN event_time IS NULL THEN 'INVALID_EVENT_TIME' ELSE 'UNKNOWN_CONTRACT_VIOLATION' END AS reject_reason
        FROM financial_transactions_raw WHERE transaction_id IS NULL OR event_time_epoch_us IS NULL OR event_time IS NULL
    """)


def register_iceberg_catalog_and_tables(table_env: "StreamTableEnvironment", config: JobConfig) -> None:
    catalog_name = sql_identifier(config.iceberg_catalog_name)
    table_env.execute_sql(f"""
        CREATE CATALOG {catalog_name} WITH (
            'type' = 'iceberg', 'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
            'uri' = {sql_literal(config.iceberg_rest_uri)}, 'warehouse' = {sql_literal(config.iceberg_warehouse)},
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', 's3.endpoint' = {sql_literal(config.s3_endpoint)},
            's3.path-style-access' = 'true', 's3.region' = {sql_literal(config.s3_region)}
        )
    """)


def submit_medallion_inserts(table_env: "StreamTableEnvironment", config: JobConfig) -> None:
    catalog_name = sql_identifier(config.iceberg_catalog_name)
    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(f"INSERT INTO {catalog_name}.bronze.transactions SELECT transaction_id, account_id, amount, currency, event_time_epoch_us, device_id, location, is_flagged_suspicious, CASE WHEN event_time IS NULL THEN NULL ELSE CAST(event_time AS TIMESTAMP(6)) END AS event_time, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS ingest_time, CAST(EXTRACT(YEAR FROM COALESCE(event_time, CURRENT_TIMESTAMP)) AS INT) AS `year`, CAST(EXTRACT(MONTH FROM COALESCE(event_time, CURRENT_TIMESTAMP)) AS INT) AS `month`, CAST(EXTRACT(DAY FROM COALESCE(event_time, CURRENT_TIMESTAMP)) AS INT) AS `day`, CAST(EXTRACT(HOUR FROM COALESCE(event_time, CURRENT_TIMESTAMP)) AS INT) AS `hour` FROM financial_transactions_valid")
    statement_set.add_insert_sql(f"INSERT INTO {catalog_name}.bronze.transactions_rejected SELECT transaction_id, account_id, amount, currency, event_time_epoch_us, device_id, location, is_flagged_suspicious, CAST(event_time AS TIMESTAMP(6)) AS event_time, reject_reason, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS rejected_at FROM financial_transactions_invalid")
    statement_set.add_insert_sql(f"INSERT INTO {catalog_name}.silver.transactions SELECT transaction_id, CONCAT('ACC-SHA256-', SUBSTRING(SHA2(CONCAT({sql_literal(config.pii_hash_salt)}, ':', COALESCE(account_id, '')), 256), 1, 16)) AS account_id, amount, currency, event_time_epoch_us, CONCAT('DEV-SHA256-', SUBSTRING(SHA2(CONCAT({sql_literal(config.pii_hash_salt)}, ':', COALESCE(device_id, '')), 256), 1, 16)) AS device_id, location, is_flagged_suspicious, CAST(event_time AS TIMESTAMP(6)) AS event_time, CAST(window_start AS TIMESTAMP(6)) AS dedup_window_start, CAST(window_end AS TIMESTAMP(6)) AS dedup_window_end, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS ingest_time, CAST(EXTRACT(YEAR FROM event_time) AS INT) AS `year`, CAST(EXTRACT(MONTH FROM event_time) AS INT) AS `month`, CAST(EXTRACT(DAY FROM event_time) AS INT) AS `day`, CAST(EXTRACT(HOUR FROM event_time) AS INT) AS `hour` FROM (SELECT transaction_id, window_start, window_end, MIN(account_id) AS account_id, MIN(device_id) AS device_id, MIN(amount) AS amount, MIN(currency) AS currency, MIN(event_time) AS event_time, MIN(event_time_epoch_us) AS event_time_epoch_us, MIN(location) AS location, CASE WHEN MAX(CASE WHEN is_flagged_suspicious THEN 1 ELSE 0 END) = 1 THEN TRUE ELSE FALSE END AS is_flagged_suspicious FROM TABLE(TUMBLE(TABLE financial_transactions_valid, DESCRIPTOR(event_time), INTERVAL '{config.dedup_window_minutes}' MINUTES)) GROUP BY window_start, window_end, transaction_id)")
    table_result = statement_set.execute()
    LOGGER.info("Submitted Bronze/Silver Iceberg inserts. Silver rows are emitted after each %s minute event-time window closes plus %s seconds of allowed lateness.", config.dedup_window_minutes, config.watermark_lateness_seconds)
    if config.wait_for_job:
        table_result.wait()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


if __name__ == "__main__":
    main()

