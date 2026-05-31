"""Initialize Iceberg namespaces and streaming table contracts through Trino."""
from __future__ import annotations
import json, logging, os, time, urllib.error, urllib.request
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger("iceberg-init")

@dataclass(frozen=True)
class InitConfig:
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
            trino_statement_url=os.getenv("TRINO_STATEMENT_URL", cls.trino_statement_url), trino_user=os.getenv("TRINO_USER", cls.trino_user), trino_catalog=os.getenv("TRINO_CATALOG", cls.trino_catalog),
            retry_attempts=max(1, _env_val("ICEBERG_INIT_RETRY_ATTEMPTS", cls.retry_attempts, int)), retry_delay_seconds=max(0.1, _env_val("ICEBERG_INIT_RETRY_DELAY_SECONDS", cls.retry_delay_seconds, float)),
            ddl_retry_attempts=max(1, _env_val("ICEBERG_INIT_DDL_RETRY_ATTEMPTS", cls.ddl_retry_attempts, int)), ddl_retry_delay_seconds=max(0.1, _env_val("ICEBERG_INIT_DDL_RETRY_DELAY_SECONDS", cls.ddl_retry_delay_seconds, float)), log_level=os.getenv("LOG_LEVEL", cls.log_level)
        )

class TrinoClient:
    def __init__(self, config: InitConfig) -> None: self.config = config
    def execute(self, sql: str) -> dict[str, Any]:
        res = self._raise_for_trino_error(self._request(self.config.trino_statement_url, sql.encode("utf-8")), sql)
        while res.get("nextUri"):
            time.sleep(0.1)
            res = self._raise_for_trino_error(self._request(res["nextUri"]), sql)
        return res
    def execute_with_retry(self, sql: str) -> dict[str, Any]:
        last_error = None
        for attempt in range(1, self.config.ddl_retry_attempts + 1):
            try: return self.execute(sql)
            except Exception as exc:
                last_error = exc; LOGGER.warning("DDL failed %s/%s: %s", attempt, self.config.ddl_retry_attempts, exc)
                if attempt < self.config.ddl_retry_attempts: time.sleep(self.config.ddl_retry_delay_seconds)
        raise RuntimeError("DDL failed") from last_error
    def wait_until_ready(self) -> None:
        for attempt in range(1, self.config.retry_attempts + 1):
            try: self.execute(f"SHOW SCHEMAS FROM {self.config.trino_catalog}"); LOGGER.info("Trino/Iceberg ready"); return
            except Exception as exc: LOGGER.warning("Waiting %s/%s: %s", attempt, self.config.retry_attempts, exc); time.sleep(self.config.retry_delay_seconds)
        raise RuntimeError("Trino not ready")
    def _request(self, url: str, data: bytes | None = None) -> dict[str, Any]:
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "text/plain", "X-Trino-User": self.config.trino_user, "X-Trino-Catalog": self.config.trino_catalog, "X-Trino-Source": "fintech-iceberg-init"}, method="POST" if data is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp: return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc: raise RuntimeError(f"Trino HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')}") from exc
        except urllib.error.URLError as exc: raise RuntimeError(f"Trino error {url}: {exc}") from exc
    @staticmethod
    def _raise_for_trino_error(response: dict[str, Any], sql: str) -> dict[str, Any]:
        if "error" in response:
            err = response["error"]
            raise RuntimeError(f"{err.get('errorName', 'UNKNOWN')}: {err.get('message', 'unknown Trino error')}")
        return response

def main() -> None:
    config = InitConfig.from_env()
    configure_logging(config.log_level)
    LOGGER.info("Initializing Iceberg catalog=%s via Trino=%s user=%s", config.trino_catalog, config.trino_statement_url, config.trino_user)
    client = TrinoClient(config)
    client.wait_until_ready()
    for stmt in build_init_statements(config.trino_catalog):
        client.execute_with_retry(stmt)
        LOGGER.info("Applied: %s", " ".join(stmt.split()))

def build_init_statements(catalog: str) -> list[str]:
    cat = sql_identifier(catalog)
    t_c = ["transaction_id varchar NOT NULL", "account_id varchar NOT NULL", "amount decimal(18, 2)", "currency varchar", "event_time_epoch_us bigint", "device_id varchar", "location varchar", "is_flagged_suspicious boolean", "event_time timestamp(6)"]
    tables = {
        "bronze.transactions": (t_c + ["ingest_time timestamp(6)", "year integer", "month integer", "day integer", "hour integer"], "ARRAY['year', 'month', 'day', 'hour']", "ARRAY['event_time', 'transaction_id']", "MAP(ARRAY['write.data.path'], ARRAY['s3://warehouse/data'])"),
        "silver.transactions": (t_c + ["dedup_window_start timestamp(6)", "dedup_window_end timestamp(6)", "ingest_time timestamp(6)", "year integer", "month integer", "day integer", "hour integer"], "ARRAY['year', 'month', 'day', 'hour']", "ARRAY['event_time', 'transaction_id']", "MAP(ARRAY['write.upsert.enabled', 'write.data.path'], ARRAY['false', 's3://warehouse/data'])"),
        "bronze.transactions_rejected": (["transaction_id varchar", "account_id varchar"] + t_c[2:] + ["reject_reason varchar", "rejected_at timestamp(6)"], None, "ARRAY['rejected_at', 'reject_reason']", "MAP(ARRAY['write.data.path'], ARRAY['s3://warehouse/data'])")
    }
    ddls = []
    for name, (cols, part, sort, extra) in tables.items():
        props = ["format = 'PARQUET'", "format_version = 2", "compression_codec = 'ZSTD'", f"sorted_by = {sort}", "max_commit_retry = 10", "delete_after_commit_enabled = true", "max_previous_versions = 20", "object_store_layout_enabled = true", f"extra_properties = {extra}"]
        if part: props.insert(3, f"partitioning = {part}")
        ddls.append(f"CREATE TABLE IF NOT EXISTS {cat}.{name} ({','.join(cols)}) WITH ({','.join(props)})")
    return [f"CREATE SCHEMA IF NOT EXISTS {cat}.{s}" for s in ["bronze", "silver", "gold"]] + ddls

def configure_logging(lvl: str) -> None: logging.basicConfig(level=getattr(logging, lvl.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(name)s %(message)s")
def compact_sql(sql: str) -> str: return " ".join(sql.split())
def sql_identifier(val: str) -> str:
    if not val or not val.replace("_", "").isalnum() or val[0].isdigit(): raise ValueError(f"Unsafe identifier: {val!r}")
    return val
def _env_val(name: str, default: Any, cast: type) -> Any:
    try: return cast(os.getenv(name, str(default)))
    except: return default

if __name__ == "__main__":
    main()
