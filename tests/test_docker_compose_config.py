import unittest
from pathlib import Path


class DockerComposeConfigTests(unittest.TestCase):
    def test_iceberg_rest_uses_postgres_catalog_backend(self) -> None:
        compose = Path("docker/docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("iceberg-catalog-db:", compose)
        self.assertIn("image: postgres:16-alpine", compose)
        self.assertIn("dockerfile: iceberg-rest/Dockerfile", compose)
        self.assertIn("image: fintech-iceberg-rest:1.10.1-postgres", compose)
        self.assertIn(
            "CATALOG_URI: jdbc:postgresql://iceberg-catalog-db:5432/${ICEBERG_CATALOG_DB_NAME:-iceberg}",
            compose,
        )
        self.assertIn("CATALOG_JDBC_USER: ${ICEBERG_CATALOG_DB_USER:-iceberg}", compose)
        self.assertIn(
            "CATALOG_JDBC_PASSWORD: ${ICEBERG_CATALOG_DB_PASSWORD:?ICEBERG_CATALOG_DB_PASSWORD is required}",
            compose,
        )
        self.assertIn("iceberg_catalog_db:/var/lib/postgresql/data", compose)
        self.assertNotIn("jdbc:sqlite", compose)
        self.assertNotIn("iceberg_catalog:/catalog", compose)

    def test_iceberg_rest_image_adds_postgres_jdbc_driver_to_classpath(self) -> None:
        dockerfile = Path("docker/iceberg-rest/Dockerfile").read_text(encoding="utf-8")

        self.assertIn("POSTGRES_JDBC_VERSION=42.7.4", dockerfile)
        self.assertIn("org/postgresql/postgresql", dockerfile)
        self.assertIn("/usr/lib/iceberg-rest/postgresql.jar", dockerfile)
        self.assertIn("iceberg-rest-adapter.jar:postgresql.jar", dockerfile)
        self.assertIn("org.apache.iceberg.rest.RESTCatalogServer", dockerfile)

    def test_flink_checkpoints_are_stored_in_minio_not_docker_volume(self) -> None:
        compose = Path("docker/docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn(
            "FLINK_CHECKPOINT_DIR: s3://warehouse/flink-checkpoints/kafka-to-iceberg",
            compose,
        )
        self.assertNotIn("FLINK_CHECKPOINT_DIR: file://", compose)
        self.assertNotIn("flink-checkpoints-init", compose)
        self.assertNotIn("flink_checkpoints", compose)
        self.assertNotIn("/opt/flink/checkpoints", compose)

    def test_flink_image_enables_s3_filesystem_plugin(self) -> None:
        dockerfile = Path("docker/flink/Dockerfile").read_text(encoding="utf-8")

        self.assertIn("flink-s3-fs-hadoop-*.jar", dockerfile)
        self.assertIn("/opt/flink/lib/", dockerfile)


if __name__ == "__main__":
    unittest.main()
