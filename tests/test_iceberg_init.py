import os
import unittest
from unittest.mock import patch

from storage import iceberg_init


class IcebergInitTests(unittest.TestCase):
    def test_build_init_statements_creates_expected_contracts(self) -> None:
        statements = iceberg_init.build_init_statements("iceberg")
        rendered = "\n".join(iceberg_init.compact_sql(statement) for statement in statements)

        self.assertIn("CREATE SCHEMA IF NOT EXISTS iceberg.bronze", rendered)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS iceberg.silver", rendered)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS iceberg.gold", rendered)
        self.assertIn("CREATE TABLE IF NOT EXISTS iceberg.bronze.transactions", rendered)
        self.assertIn("CREATE TABLE IF NOT EXISTS iceberg.bronze.transactions_rejected", rendered)
        self.assertIn("CREATE TABLE IF NOT EXISTS iceberg.silver.transactions", rendered)
        self.assertIn("amount decimal(18, 2)", rendered)
        self.assertIn("event_time_epoch_us bigint", rendered)
        self.assertIn("reject_reason varchar", rendered)
        self.assertNotIn('"timestamp" varchar', rendered)
        self.assertIn("compression_codec = 'ZSTD'", rendered)
        self.assertIn("partitioning = ARRAY['year', 'month', 'day', 'hour']", rendered)
        self.assertIn("object_store_layout_enabled = true", rendered)
        self.assertNotIn("gold_hourly_liquidity", rendered)

    def test_build_init_statements_rejects_unsafe_catalog(self) -> None:
        with self.assertRaises(ValueError):
            iceberg_init.build_init_statements("iceberg;drop")

    def test_init_config_from_env(self) -> None:
        env = {
            "TRINO_STATEMENT_URL": "http://trino:8080/v1/statement",
            "TRINO_USER": "ci",
            "TRINO_CATALOG": "lakehouse",
            "ICEBERG_INIT_RETRY_ATTEMPTS": "0",
            "ICEBERG_INIT_RETRY_DELAY_SECONDS": "0",
            "ICEBERG_INIT_DDL_RETRY_ATTEMPTS": "0",
            "ICEBERG_INIT_DDL_RETRY_DELAY_SECONDS": "0",
        }

        with patch.dict(os.environ, env, clear=True):
            config = iceberg_init.InitConfig.from_env()

        self.assertEqual(config.trino_statement_url, "http://trino:8080/v1/statement")
        self.assertEqual(config.trino_user, "ci")
        self.assertEqual(config.trino_catalog, "lakehouse")
        self.assertEqual(config.retry_attempts, 1)
        self.assertEqual(config.retry_delay_seconds, 0.1)
        self.assertEqual(config.ddl_retry_attempts, 1)
        self.assertEqual(config.ddl_retry_delay_seconds, 0.1)

    def test_trino_error_response_raises_runtime_error(self) -> None:
        response = {
            "error": {
                "errorName": "TABLE_NOT_FOUND",
                "message": "missing table",
            }
        }

        with self.assertRaisesRegex(RuntimeError, "TABLE_NOT_FOUND: missing table"):
            iceberg_init.TrinoClient._raise_for_trino_error(response, "select 1")


if __name__ == "__main__":
    unittest.main()
