"""Job configuration logic for Flink streaming jobs."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger("fintech.kafka-to-iceberg-config")


@dataclass(frozen=True)
class JobConfig:
    """Runtime configuration loaded from environment variables."""

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    source_topic: str = "financial.transactions"
    schema_registry_subject: str = "financial.transactions-value"
    consumer_group_id: str = "flink-kafka-to-iceberg-phase4"
    startup_mode: str = "group-offsets"
    iceberg_catalog_name: str = "iceberg"
    iceberg_rest_uri: str = "http://localhost:8181"
    iceberg_warehouse: str = "s3://warehouse/"
    s3_endpoint: str = "http://localhost:9000"
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = "us-east-1"
    pii_hash_salt: str | None = None
    dedup_window_minutes: int = 1
    watermark_lateness_seconds: int = 5
    checkpoint_interval_ms: int = 300_000
    checkpoint_min_pause_ms: int = 10_000
    checkpoint_timeout_ms: int = 120_000
    checkpoint_dir: str = "s3://warehouse/flink-checkpoints/kafka-to-iceberg"
    rocksdb_local_dir: str = "/tmp/flink/rocksdb/kafka-to-iceberg"
    parallelism: int = 2
    pipeline_jars: str | None = None
    table_state_ttl: str = "12 min"
    source_idle_timeout: str = "30 s"
    wait_for_job: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "JobConfig":
        """Create a config object from env vars with local-safe defaults."""

        source_topic = os.getenv("KAFKA_TOPIC", cls.source_topic)
        dedup_window_minutes = max(
            1,
            _int_env(
                "FLINK_DEDUP_WINDOW_MINUTES",
                _int_env("DEDUP_WINDOW_MINUTES", cls.dedup_window_minutes),
            ),
        )
        watermark_lateness_seconds = max(
            0,
            _int_env(
                "FLINK_WATERMARK_LATENESS_SECONDS",
                _int_env("WATERMARK_LATENESS_SECONDS", cls.watermark_lateness_seconds),
            ),
        )
        default_state_ttl_minutes = (
            dedup_window_minutes
            + math.ceil(watermark_lateness_seconds / 60)
            + 5
        )

        s3_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        s3_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        pii_hash_salt = os.getenv("PII_HASH_SALT")

        if not s3_access_key_id:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")
        if not s3_secret_access_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is required")
        if not pii_hash_salt:
            raise ValueError("PII_HASH_SALT environment variable is required")
        if len(pii_hash_salt) < 32 or not all(
            c in "0123456789abcdefABCDEF" for c in pii_hash_salt
        ):
            raise ValueError(
                "PII_HASH_SALT must be at least 32 hex characters. "
                'Generate one with: python -c "import secrets; print(secrets.token_hex(32))"'
            )

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
            source_idle_timeout=os.getenv(
                "FLINK_SOURCE_IDLE_TIMEOUT", cls.source_idle_timeout
            ),
            wait_for_job=_bool_env("FLINK_WAIT_FOR_JOB", cls.wait_for_job),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


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


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False

    LOGGER.warning("Invalid boolean for %s=%r; using default %s", name, value, default)
    return default
