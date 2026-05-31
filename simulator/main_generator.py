"""High-throughput Kafka Avro transaction simulator.

The simulator produces realistic financial transaction events to Kafka and uses
Confluent Schema Registry to register and encode records with the Avro contract
in ``simulator/schemas/transaction.avsc``.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

try:
    from confluent_kafka import KafkaError, Producer
    from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.schema_registry.error import SchemaRegistryError
    from confluent_kafka.serialization import (
        MessageField,
        SerializationContext,
        StringSerializer,
    )
except ImportError:  
    KafkaError = Any 
    Producer = None 
    Schema = None 
    SchemaRegistryClient = None 
    AvroSerializer = None 
    MessageField = None 
    SerializationContext = None
    StringSerializer = None 

    class SchemaRegistryError(Exception):
        """Fallback so contract tests can import this module without Kafka deps."""


try:
    from faker import Faker
except ImportError: 
    Faker = None 

try:
    from fastavro import parse_schema
except ImportError: 
    parse_schema = None 


LOGGER = logging.getLogger("transaction-simulator")
DEFAULT_SCHEMA_PATH = Path(__file__).parent / "schemas" / "transaction.avsc"
MONEY_QUANTUM = Decimal("0.01")
DEFAULT_SCHEMA_COMPATIBILITY = "BACKWARD_TRANSITIVE"
ALLOWED_SCHEMA_COMPATIBILITY_MODES = {
    "NONE",
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
}


@dataclass(frozen=True)
class SimulatorConfig:
    """Runtime configuration loaded from environment variables."""

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "financial.transactions"
    schema_registry_subject: str = "financial.transactions-value"
    schema_registry_compatibility: str = DEFAULT_SCHEMA_COMPATIBILITY
    enforce_schema_registry_compatibility: bool = True
    schema_path: Path = DEFAULT_SCHEMA_PATH
    target_events_per_second: int = 75
    worker_count: int = 2
    anomaly_rate: float = 0.05
    linger_ms: int = 20
    batch_num_messages: int = 10_000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "SimulatorConfig":
        """Create configuration from environment variables with safe defaults."""

        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", cls.schema_registry_url),
            topic=os.getenv("KAFKA_TOPIC", cls.topic),
            schema_registry_subject=os.getenv(
                "SCHEMA_REGISTRY_SUBJECT",
                f"{os.getenv('KAFKA_TOPIC', cls.topic)}-value",
            ),
            schema_registry_compatibility=normalize_schema_registry_compatibility(
                os.getenv(
                    "SCHEMA_REGISTRY_COMPATIBILITY",
                    cls.schema_registry_compatibility,
                )
            ),
            enforce_schema_registry_compatibility=_bool_env(
                "ENFORCE_SCHEMA_REGISTRY_COMPATIBILITY",
                cls.enforce_schema_registry_compatibility,
            ),
            schema_path=Path(os.getenv("TRANSACTION_SCHEMA_PATH", str(DEFAULT_SCHEMA_PATH))),
            target_events_per_second=_int_env("TARGET_EVENTS_PER_SECOND", cls.target_events_per_second),
            worker_count=max(1, _int_env("SIMULATOR_WORKER_COUNT", cls.worker_count)),
            anomaly_rate=_float_env("ANOMALY_RATE", cls.anomaly_rate),
            linger_ms=_int_env("KAFKA_LINGER_MS", cls.linger_ms),
            batch_num_messages=_int_env("KAFKA_BATCH_NUM_MESSAGES", cls.batch_num_messages),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )


class TransactionFactory:
    """Generate normal and anomalous financial transaction payloads."""

    currencies = ("USD", "THB", "EUR", "SGD", "JPY")
    merchant_categories = (
        "grocery",
        "restaurant",
        "ride_hailing",
        "ecommerce",
        "utilities",
        "airline",
        "atm_withdrawal",
        "wire_transfer",
    )

    def __init__(self, anomaly_rate: float) -> None:
        faker_cls = _require_faker()
        self.fake = faker_cls()
        self.anomaly_rate = min(max(anomaly_rate, 0.0), 1.0)
        self.hot_accounts = [f"acct_{uuid.uuid4().hex[:12]}" for _ in range(25)]
        self.hot_devices = [f"dev_{uuid.uuid4().hex[:12]}" for _ in range(10)]

    def create(self) -> dict[str, Any]:
        """Create one Avro-compatible transaction record."""

        if random.random() < self.anomaly_rate:
            return self._create_anomaly()
        return self._create_normal()

    def _base_record(self, *, flagged: bool) -> dict[str, Any]:
        city = self.fake.city().replace(",", "")
        country = self.fake.country_code()
        now = datetime.now(UTC)
        return {
            "transaction_id": str(uuid.uuid4()),
            "account_id": f"acct_{uuid.uuid4().hex[:12]}",
            "amount": Decimal("0.00"),
            "currency": random.choice(self.currencies),
            "event_time_epoch_us": epoch_microseconds(now),
            "device_id": f"dev_{uuid.uuid4().hex[:12]}",
            "location": f"{city}, {country}",
            "is_flagged_suspicious": flagged,
        }

    def _create_normal(self) -> dict[str, Any]:
        record = self._base_record(flagged=False)
        category = random.choice(self.merchant_categories)

        if category == "wire_transfer":
            amount = random.uniform(250.0, 4_500.0)
        elif category == "airline":
            amount = random.uniform(120.0, 2_800.0)
        elif category == "atm_withdrawal":
            amount = random.choice((20, 40, 60, 100, 200, 500))
        else:
            amount = random.lognormvariate(mu=3.3, sigma=0.75)

        record["amount"] = _money(max(amount, 1.0))
        return record

    def _create_anomaly(self) -> dict[str, Any]:
        record = self._base_record(flagged=True)
        anomaly_type = random.choice(("high_amount", "rapid_fire", "geo_velocity"))

        if anomaly_type == "high_amount":
            record["amount"] = _money(random.uniform(25_000.0, 500_000.0))
            record["account_id"] = random.choice(self.hot_accounts)
        elif anomaly_type == "rapid_fire":
            record["amount"] = _money(random.uniform(1.0, 250.0))
            record["account_id"] = random.choice(self.hot_accounts)
            record["device_id"] = random.choice(self.hot_devices)
        else:
            record["amount"] = _money(random.uniform(500.0, 8_000.0))
            record["account_id"] = random.choice(self.hot_accounts)
            record["location"] = random.choice(("Bangkok, TH", "New York, US", "Lagos, NG", "Sao Paulo, BR"))

        return record


class TransactionProducer:
    """Serialize transaction records with Avro and publish them to Kafka."""

    def __init__(self, config: SimulatorConfig) -> None:
        self.config = config
        _require_confluent_kafka()
        self.producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "linger.ms": config.linger_ms,
                "batch.num.messages": config.batch_num_messages,
                "enable.idempotence": True,
                "acks": "all",
                "retries": 10,
                "compression.type": "snappy",
            }
        )
        self.key_serializer = StringSerializer("utf_8")
        self.value_serializer = self._build_avro_serializer()
        self.delivered_count = 0
        self.failed_count = 0
        self._counter_lock = threading.Lock()

    def produce(self, record: dict[str, Any]) -> bool:
        """Serialize and enqueue one record for Kafka delivery."""

        context = SerializationContext(self.config.topic, MessageField.VALUE)
        key_context = SerializationContext(self.config.topic, MessageField.KEY)

        try:
            key = self.key_serializer(record["transaction_id"], key_context)
            value = self.value_serializer(record, context)
            self.producer.produce(
                self.config.topic,
                key=key,
                value=value,
                on_delivery=self._delivery_report,
            )
            self.producer.poll(0)
            return True
        except BufferError:
            LOGGER.warning("Producer queue is full; polling before retry")
            self.producer.poll(1.0)
            self.mark_failed()
            return False
        except SchemaRegistryError as exc:
            LOGGER.error("Schema Registry error while serializing record: %s", exc, exc_info=True)
            self.mark_failed()
            return False
        except Exception as exc:
            LOGGER.error("Failed to serialize or enqueue record: %s", exc, exc_info=True)
            self.mark_failed()
            return False

    def flush(self, timeout: float = 30.0) -> None:
        """Flush outstanding messages before shutdown."""

        remaining = self.producer.flush(timeout)
        if remaining:
            LOGGER.warning("Producer shutdown with %s undelivered message(s)", remaining)

    def mark_failed(self) -> None:
        """Record a failed enqueue or serialization attempt."""

        with self._counter_lock:
            self.failed_count += 1

    def _build_avro_serializer(self) -> AvroSerializer:
        if not self.config.schema_path.exists():
            raise FileNotFoundError(f"Avro schema not found: {self.config.schema_path}")

        schema_str = self.config.schema_path.read_text(encoding="utf-8")
        _validate_avro_schema(schema_str, self.config.schema_path)

        schema_registry_conf = {"url": self.config.schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        if self.config.enforce_schema_registry_compatibility:
            ensure_schema_registry_compatibility(
                schema_registry_client=schema_registry_client,
                subject=self.config.schema_registry_subject,
                compatibility=self.config.schema_registry_compatibility,
                schema_str=schema_str,
            )
        return AvroSerializer(schema_registry_client, schema_str, conf={"auto.register.schemas": True})

    def _delivery_report(self, error: KafkaError | None, message: Any) -> None:
        with self._counter_lock:
            if error is not None:
                self.failed_count += 1
                LOGGER.error(
                    "Kafka delivery failed topic=%s partition=%s key=%r error=%s",
                    message.topic(),
                    message.partition(),
                    message.key(),
                    error,
                )
                return

            self.delivered_count += 1
            if self.delivered_count % 1_000 == 0:
                LOGGER.info(
                    "Delivered %s records to %s [%s] offset=%s",
                    self.delivered_count,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )


def run_worker(
    worker_id: int,
    config: SimulatorConfig,
    producer: TransactionProducer,
    stop_event: threading.Event,
) -> None:
    """Generate and produce records at this worker's share of the target rate."""

    factory = TransactionFactory(config.anomaly_rate)
    per_worker_rate = max(config.target_events_per_second / config.worker_count, 1.0)
    sleep_seconds = 1.0 / per_worker_rate
    LOGGER.info("Worker %s started at %.2f events/sec", worker_id, per_worker_rate)

    while not stop_event.is_set():
        start = time.perf_counter()
        record = factory.create()

        if not producer.produce(record):
            LOGGER.debug("Worker %s skipped one record after produce failure", worker_id)

        elapsed = time.perf_counter() - start
        delay = max(0.0, sleep_seconds - elapsed)
        if stop_event.wait(delay):
            break

    LOGGER.info("Worker %s stopped", worker_id)


def main() -> None:
    """Entrypoint for the transaction simulator."""

    config = SimulatorConfig.from_env()
    configure_logging(config.log_level)
    LOGGER.info(
        "Starting simulator topic=%s kafka=%s schema_registry=%s eps=%s workers=%s anomaly_rate=%.3f",
        config.topic,
        config.bootstrap_servers,
        config.schema_registry_url,
        config.target_events_per_second,
        config.worker_count,
        config.anomaly_rate,
    )

    stop_event = threading.Event()
    install_signal_handlers(stop_event)

    try:
        producer = TransactionProducer(config)
    except Exception as exc:
        LOGGER.critical("Failed to initialize Kafka Avro producer: %s", exc, exc_info=True)
        raise SystemExit(1) from exc

    threads = [
        threading.Thread(
            target=run_worker,
            name=f"transaction-worker-{worker_id}",
            args=(worker_id, config, producer, stop_event),
            daemon=True,
        )
        for worker_id in range(1, config.worker_count + 1)
    ]

    for thread in threads:
        thread.start()

    try:
        while not stop_event.is_set():
            time.sleep(5)
            LOGGER.info(
                "Simulator heartbeat delivered=%s failed=%s",
                producer.delivered_count,
                producer.failed_count,
            )
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=10)
        producer.flush()
        LOGGER.info(
            "Simulator stopped delivered=%s failed=%s",
            producer.delivered_count,
            producer.failed_count,
        )


def configure_logging(level: str) -> None:
    """Configure compact structured logging for local and container runs."""

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(threadName)s %(message)s",
    )


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Stop workers gracefully on SIGINT or SIGTERM."""

    def _handler(signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s; stopping simulator", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def epoch_microseconds(value: datetime) -> int:
    """Return exact Unix epoch microseconds for an aware datetime."""

    if value.tzinfo is None:
        raise ValueError("event time must be timezone-aware")

    utc_value = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc_value - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def ensure_schema_registry_compatibility(
    *,
    schema_registry_client: Any,
    subject: str,
    compatibility: str,
    schema_str: str,
) -> None:
    """Pin and preflight the subject compatibility policy before producing."""

    compatibility = normalize_schema_registry_compatibility(compatibility)
    schema_registry_client.set_compatibility(
        subject_name=subject,
        level=compatibility,
    )
    LOGGER.info(
        "Schema Registry subject=%s compatibility=%s",
        subject,
        compatibility,
    )

    if Schema is None:
        return

    schema = Schema(schema_str, "AVRO")
    try:
        schema_registry_client.get_latest_version(subject)
    except SchemaRegistryError as exc:
        if _schema_registry_error_code(exc) in {40401, 40403}:
            return
        raise

    if not schema_registry_client.test_compatibility(subject, schema, version="latest"):
        raise ValueError(
            f"Avro schema is not {compatibility} compatible with latest subject "
            f"{subject!r}"
        )


def normalize_schema_registry_compatibility(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in ALLOWED_SCHEMA_COMPATIBILITY_MODES:
        raise ValueError(f"Unsupported Schema Registry compatibility mode: {value!r}")
    return normalized


def _validate_avro_schema(schema_str: str, schema_path: Path) -> dict[str, Any]:
    try:
        schema_dict = json.loads(schema_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Avro schema JSON at {schema_path}: {exc}") from exc

    if parse_schema is not None:
        try:
            parse_schema(schema_dict)
        except Exception as exc:
            raise ValueError(f"Invalid Avro schema semantics at {schema_path}: {exc}") from exc
    else:
        LOGGER.warning(
            "fastavro is not installed; falling back to structural schema checks only"
        )

    validate_transaction_schema_contract(schema_dict, schema_path)
    return schema_dict


def validate_transaction_schema_contract(
    schema_dict: dict[str, Any],
    schema_path: Path,
) -> None:
    """Validate fields that must stay aligned across Kafka, Flink, and Iceberg."""

    if schema_dict.get("type") != "record":
        raise ValueError(f"Transaction schema must be an Avro record: {schema_path}")

    fields = {
        field.get("name"): field
        for field in schema_dict.get("fields", [])
        if isinstance(field, dict)
    }
    required_fields = {
        "transaction_id",
        "account_id",
        "amount",
        "currency",
        "event_time_epoch_us",
        "device_id",
        "location",
        "is_flagged_suspicious",
    }
    missing_fields = sorted(required_fields - fields.keys())
    if missing_fields:
        raise ValueError(
            f"Transaction schema missing required fields {missing_fields}: {schema_path}"
        )

    if "timestamp" in fields:
        raise ValueError(
            "Transaction schema must not carry redundant timestamp string; "
            "event_time_epoch_us is authoritative"
        )

    for field_name in ("transaction_id", "account_id"):
        if fields[field_name].get("type") != "string":
            raise ValueError(f"{field_name} must be a strictly non-nullable Avro string")

    if fields["event_time_epoch_us"].get("type") != "long":
        raise ValueError("event_time_epoch_us must be a non-null Avro long")

    amount_type = fields["amount"].get("type")
    if not isinstance(amount_type, dict):
        raise ValueError("amount must be an Avro decimal logical type")

    expected_amount_contract = {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 18,
        "scale": 2,
    }
    for key, expected_value in expected_amount_contract.items():
        if amount_type.get(key) != expected_value:
            raise ValueError(
                f"amount {key} must be {expected_value!r}, got "
                f"{amount_type.get(key)!r}"
            )


def _schema_registry_error_code(exc: Exception) -> int | None:
    error_code = getattr(exc, "error_code", None)
    if callable(error_code):
        return error_code()
    if isinstance(error_code, int):
        return error_code
    return None


def _require_confluent_kafka() -> None:
    if (
        Producer is None
        or SchemaRegistryClient is None
        or AvroSerializer is None
        or MessageField is None
        or SerializationContext is None
        or StringSerializer is None
    ):
        raise RuntimeError(
            "confluent-kafka[avro] is required to run the simulator. "
            "Install dependencies with `python -m pip install -r requirements.txt`."
        )


def _require_faker() -> Any:
    if Faker is None:
        raise RuntimeError(
            "faker is required to generate simulator records. "
            "Install dependencies with `python -m pip install -r requirements.txt`."
        )
    return Faker


def _money(value: float | int | str | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


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


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False

    LOGGER.warning("Invalid boolean for %s=%r; using default %s", name, value, default)
    return default


if __name__ == "__main__":
    main()
