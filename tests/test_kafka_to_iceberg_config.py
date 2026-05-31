import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from streaming.jobs import kafka_to_iceberg as job


class KafkaToIcebergConfigTests(unittest.TestCase):
    def test_sql_literal_escapes_single_quotes(self) -> None:
        self.assertEqual(job.sql_literal("topic's-value"), "'topic''s-value'")

    def test_sql_identifier_accepts_safe_names(self) -> None:
        self.assertEqual(job.sql_identifier("iceberg_catalog_1"), "iceberg_catalog_1")

    def test_sql_identifier_rejects_unsafe_names(self) -> None:
        for value in ("", "1iceberg", "iceberg-rest", "iceberg.gold", "iceberg;drop"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    job.sql_identifier(value)

    def test_normalize_pipeline_jars_accepts_paths_and_uris(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            jar_path = Path(tmp_dir) / "flink-connector.jar"
            jar_path.touch()

            normalized = job._normalize_pipeline_jars(
                f"{jar_path};https://repo.example/flink-iceberg.jar"
            )

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertIn(jar_path.resolve().as_uri(), normalized)
        self.assertIn("https://repo.example/flink-iceberg.jar", normalized)

    def test_discover_pipeline_jars_scans_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            jars_dir = Path(tmp_dir)
            first_jar = jars_dir / "a.jar"
            second_jar = jars_dir / "b.jar"
            first_jar.touch()
            second_jar.touch()

            discovered = job._discover_pipeline_jars(jars_dir)

        self.assertIsNotNone(discovered)
        assert discovered is not None
        self.assertEqual(
            discovered,
            ";".join([first_jar.resolve().as_uri(), second_jar.resolve().as_uri()]),
        )

    def test_job_config_from_env_clamps_operational_values(self) -> None:
        env = {
            "KAFKA_TOPIC": "financial.test",
            "DEDUP_WINDOW_MINUTES": "0",
            "WATERMARK_LATENESS_SECONDS": "-20",
            "FLINK_PARALLELISM": "0",
            "FLINK_CHECKPOINT_INTERVAL_MS": "100",
            "FLINK_CHECKPOINT_TIMEOUT_MS": "100",
            "FLINK_TABLE_STATE_TTL": "42 min",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "PII_HASH_SALT": "test-salt",
        }

        with patch.dict(os.environ, env, clear=True):
            config = job.JobConfig.from_env()

        self.assertEqual(config.source_topic, "financial.test")
        self.assertEqual(config.schema_registry_subject, "financial.test-value")
        self.assertEqual(config.dedup_window_minutes, 1)
        self.assertEqual(config.watermark_lateness_seconds, 0)
        self.assertEqual(config.parallelism, 1)
        self.assertEqual(config.checkpoint_interval_ms, 1_000)
        self.assertEqual(config.checkpoint_timeout_ms, 10_000)
        self.assertEqual(config.table_state_ttl, "42 min")
        self.assertEqual(config.s3_access_key_id, "test-key")
        self.assertEqual(config.s3_secret_access_key, "test-secret")
        self.assertEqual(config.pii_hash_salt, "test-salt")

    def test_transaction_ddl_uses_authoritative_event_time_contract(self) -> None:
        script = Path("streaming/jobs/kafka_to_iceberg.py").read_text(encoding="utf-8")
        kafka_source_source = script[
            script.index("CREATE TABLE financial_transactions_raw") : script.index(
                "CREATE TEMPORARY VIEW financial_transactions_valid"
            )
        ]
        iceberg_tables_source = script[
            script.index("def register_iceberg_catalog_and_tables") : script.index(
                "def submit_medallion_inserts"
            )
        ]

        self.assertIn("event_time_epoch_us BIGINT,", kafka_source_source)
        self.assertIn("event_time_epoch_us BIGINT NOT NULL", iceberg_tables_source)
        self.assertNotIn("`timestamp` STRING", script)

    def test_invalid_events_are_routed_to_rejected_bronze_table(self) -> None:
        script = Path("streaming/jobs/kafka_to_iceberg.py").read_text(encoding="utf-8")
        insert_source = script[
            script.index("def submit_medallion_inserts") : script.index(
                "def configure_logging"
            )
        ]

        self.assertIn("CREATE TEMPORARY VIEW financial_transactions_invalid", script)
        self.assertIn("MISSING_EVENT_TIME_EPOCH_US", script)
        self.assertIn("bronze.transactions_rejected", script)
        self.assertIn("FROM financial_transactions_valid", insert_source)
        self.assertIn("FROM financial_transactions_invalid", insert_source)
        self.assertNotIn("FROM financial_transactions_raw", insert_source)

    def test_checkpoint_configuration_has_single_authority(self) -> None:
        script = Path("streaming/jobs/kafka_to_iceberg.py").read_text(encoding="utf-8")
        build_environments_source = script[
            script.index("def build_environments") : script.index(
                "def build_flink_configuration"
            )
        ]
        build_configuration_source = script[
            script.index("def build_flink_configuration") : script.index(
                "def register_kafka_source"
            )
        ]

        self.assertNotIn("enable_checkpointing", build_environments_source)
        self.assertNotIn("get_checkpoint_config", build_environments_source)
        self.assertIn("execution.checkpointing.interval", build_configuration_source)
        self.assertIn("execution.checkpointing.min-pause", build_configuration_source)
        self.assertIn("execution.checkpointing.timeout", build_configuration_source)
        self.assertIn(
            "execution.checkpointing.max-concurrent-checkpoints",
            build_configuration_source,
        )


if __name__ == "__main__":
    unittest.main()
