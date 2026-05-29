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

    def test_job_config_from_env_clamps_operational_values(self) -> None:
        env = {
            "KAFKA_TOPIC": "financial.test",
            "DEDUP_WINDOW_MINUTES": "0",
            "WATERMARK_LATENESS_SECONDS": "-20",
            "FLINK_PARALLELISM": "0",
            "FLINK_CHECKPOINT_INTERVAL_MS": "100",
            "FLINK_CHECKPOINT_TIMEOUT_MS": "100",
            "FLINK_TABLE_STATE_TTL": "42 min",
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


if __name__ == "__main__":
    unittest.main()
