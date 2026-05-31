"""High-throughput Kafka Avro transaction simulator."""

from __future__ import annotations
import json, logging, os, random, signal, threading, time, uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaError, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from faker import Faker
from fastavro import parse_schema

LOGGER = logging.getLogger("transaction-simulator")
DEFAULT_SCHEMA_PATH = Path(__file__).parent / "schemas" / "transaction.avsc"
ALLOWED_SCHEMA_COMPATIBILITY_MODES = {"NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"}
_money = lambda v: Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
def epoch_microseconds(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("event time must be timezone-aware")
    return int(value.astimezone(UTC).timestamp() * 1000000)

def _env(name: str, default: Any, cast_type: type) -> Any:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return val.strip().lower() in {"1", "true", "yes", "y", "on"} if cast_type is bool else cast_type(val)
    except ValueError:
        return default

@dataclass(frozen=True)
class SimulatorConfig:
    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "financial.transactions"
    schema_registry_subject: str = "financial.transactions-value"
    schema_registry_compatibility: str = "BACKWARD_TRANSITIVE"
    enforce_schema_registry_compatibility: bool = True
    schema_path: Path = DEFAULT_SCHEMA_PATH
    target_events_per_second: int = 75
    worker_count: int = 2
    anomaly_rate: float = 0.05
    linger_ms: int = 20
    batch_num_messages: int = 10000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> SimulatorConfig:
        t = os.getenv("KAFKA_TOPIC", cls.topic)
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", cls.schema_registry_url),
            topic=t,
            schema_registry_subject=os.getenv("SCHEMA_REGISTRY_SUBJECT", f"{t}-value"),
            schema_registry_compatibility=os.getenv("SCHEMA_REGISTRY_COMPATIBILITY", cls.schema_registry_compatibility).upper(),
            enforce_schema_registry_compatibility=_env("ENFORCE_SCHEMA_REGISTRY_COMPATIBILITY", cls.enforce_schema_registry_compatibility, bool),
            schema_path=Path(os.getenv("TRANSACTION_SCHEMA_PATH", str(DEFAULT_SCHEMA_PATH))),
            target_events_per_second=_env("TARGET_EVENTS_PER_SECOND", cls.target_events_per_second, int),
            worker_count=max(1, _env("SIMULATOR_WORKER_COUNT", cls.worker_count, int)),
            anomaly_rate=_env("ANOMALY_RATE", cls.anomaly_rate, float),
            linger_ms=_env("KAFKA_LINGER_MS", cls.linger_ms, int),
            batch_num_messages=_env("KAFKA_BATCH_NUM_MESSAGES", cls.batch_num_messages, int),
            log_level=os.getenv("LOG_LEVEL", cls.log_level),
        )

class TransactionFactory:
    currencies = ("USD", "THB", "EUR", "SGD", "JPY")
    merchant_categories = ("grocery", "restaurant", "ride_hailing", "ecommerce", "utilities", "airline", "atm_withdrawal", "wire_transfer")
    normal_mappers = {
        "wire_transfer": lambda: random.uniform(250.0, 4500.0),
        "airline": lambda: random.uniform(120.0, 2800.0),
        "atm_withdrawal": lambda: float(random.choice((20, 40, 60, 100, 200, 500))),
    }

    def __init__(self, anomaly_rate: float) -> None:
        self.fake = Faker()
        self.anomaly_rate = min(max(anomaly_rate, 0.0), 1.0)
        self.hot_accounts = [f"acct_{uuid.uuid4().hex[:12]}" for _ in range(25)]
        self.hot_devices = [f"dev_{uuid.uuid4().hex[:12]}" for _ in range(10)]

    def _base_record(self, *, flagged: bool) -> dict[str, Any]:
        city, country = self.fake.city().replace(",", ""), self.fake.country_code()
        return {
            "transaction_id": str(uuid.uuid4()), "account_id": f"acct_{uuid.uuid4().hex[:12]}", "amount": Decimal("0.00"),
            "currency": random.choice(self.currencies), "event_time_epoch_us": epoch_microseconds(datetime.now(UTC)),
            "device_id": f"dev_{uuid.uuid4().hex[:12]}", "location": f"{city}, {country}", "is_flagged_suspicious": flagged,
        }

    def create(self) -> dict[str, Any]:
        if random.random() < self.anomaly_rate:
            record = self._base_record(flagged=True)
            record["account_id"] = random.choice(self.hot_accounts)
            atype = random.choice(("high_amount", "rapid_fire", "geo_velocity"))
            mappers = {
                "high_amount": lambda r: r.update({"amount": _money(random.uniform(25000.0, 500000.0))}),
                "rapid_fire": lambda r: r.update({"amount": _money(random.uniform(1.0, 250.0)), "device_id": random.choice(self.hot_devices)}),
                "geo_velocity": lambda r: r.update({"amount": _money(random.uniform(500.0, 8000.0)), "location": random.choice(("Bangkok, TH", "New York, US", "Lagos, NG", "Sao Paulo, BR"))}),
            }
            mappers[atype](record)
            return record
        record = self._base_record(flagged=False)
        cat = random.choice(self.merchant_categories)
        amount = self.normal_mappers.get(cat, lambda: random.lognormvariate(3.3, 0.75))()
        record["amount"] = _money(max(amount, 1.0))
        return record

class TransactionProducer:
    def __init__(self, config: SimulatorConfig) -> None:
        self.config = config
        self.producer = Producer({
            "bootstrap.servers": config.bootstrap_servers,
            "linger.ms": config.linger_ms,
            "batch.num.messages": config.batch_num_messages,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "compression.type": "snappy",
        })
        self.key_serializer = StringSerializer("utf_8")
        self.value_serializer = self._build_serializer()
        self.delivered_count = 0
        self.failed_count = 0
        self._lock = threading.Lock()

    def _build_serializer(self) -> AvroSerializer:
        schema_str = self.config.schema_path.read_text(encoding="utf-8")
        parse_schema(json.loads(schema_str))
        sr_client = SchemaRegistryClient({"url": self.config.schema_registry_url})
        if self.config.enforce_schema_registry_compatibility:
            ensure_schema_registry_compatibility(
                schema_registry_client=sr_client,
                subject=self.config.schema_registry_subject,
                compatibility=self.config.schema_registry_compatibility,
                schema_str=schema_str,
            )
        return AvroSerializer(sr_client, schema_str, conf={"auto.register.schemas": True})

    def produce(self, record: dict[str, Any]) -> bool:
        try:
            key = self.key_serializer(record["transaction_id"], SerializationContext(self.config.topic, MessageField.KEY))
            val = self.value_serializer(record, SerializationContext(self.config.topic, MessageField.VALUE))
            self.producer.produce(self.config.topic, key=key, value=val, on_delivery=self._delivery_report)
            self.producer.poll(0)
            return True
        except Exception as exc:
            msg = "Schema Registry error while serializing record transaction_id=%s: %s" if isinstance(exc, SchemaRegistryError) else "Failed to serialize or enqueue record transaction_id=%s: %s"
            LOGGER.error(msg, record.get("transaction_id", "UNKNOWN"), exc if isinstance(exc, SchemaRegistryError) else type(exc).__name__)
            with self._lock:
                self.failed_count += 1
            return False

    def flush(self, timeout: float = 30.0) -> None:
        self.producer.flush(timeout)

    def _delivery_report(self, err: KafkaError | None, msg: Any) -> None:
        with self._lock:
            if err is not None:
                self.failed_count += 1
                LOGGER.error("Kafka delivery failed: %s", err)
            else:
                self.delivered_count += 1
                if self.delivered_count % 1000 == 0:
                    LOGGER.info("Delivered %s records to %s", self.delivered_count, msg.topic())

def run_worker(worker_id: int, config: SimulatorConfig, producer: TransactionProducer, stop_event: threading.Event) -> None:
    factory = TransactionFactory(config.anomaly_rate)
    sleep_secs = 1.0 / max(config.target_events_per_second / config.worker_count, 1.0)
    LOGGER.info("Worker %s started", worker_id)
    while not stop_event.is_set():
        start = time.perf_counter()
        producer.produce(factory.create())
        if stop_event.wait(max(0.0, sleep_secs - (time.perf_counter() - start))):
            break

def main() -> None:
    config = SimulatorConfig.from_env()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    LOGGER.info("Starting simulator topic=%s eps=%s", config.topic, config.target_events_per_second)
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda s, f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda s, f: stop_event.set())
    producer = TransactionProducer(config)
    threads = [threading.Thread(target=run_worker, name=f"worker-{i}", args=(i, config, producer, stop_event), daemon=True) for i in range(1, config.worker_count + 1)]
    for t in threads:
        t.start()
    try:
        while not stop_event.is_set():
            time.sleep(5)
            LOGGER.info("Heartbeat: delivered=%s failed=%s", producer.delivered_count, producer.failed_count)
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=5)
        producer.flush()

def ensure_schema_registry_compatibility(
    *,
    schema_registry_client: Any,
    subject: str,
    compatibility: str,
    schema_str: str,
) -> None:
    compatibility = normalize_schema_registry_compatibility(compatibility)
    schema_registry_client.set_compatibility(subject_name=subject, level=compatibility)
    try:
        schema_registry_client.get_latest_version(subject)
    except SchemaRegistryError as exc:
        if getattr(exc, "error_code", None) in {40401, 40403}:
            return
        raise
    if not schema_registry_client.test_compatibility(subject, Schema(schema_str, "AVRO"), version="latest"):
        raise ValueError(f"Avro schema not compatible with latest {subject!r}")

def normalize_schema_registry_compatibility(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in ALLOWED_SCHEMA_COMPATIBILITY_MODES:
        raise ValueError(f"Unsupported compatibility mode: {value!r}")
    return normalized

if __name__ == "__main__":
    main()
