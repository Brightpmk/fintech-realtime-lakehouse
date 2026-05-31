import json
import unittest
from datetime import UTC, datetime
from pathlib import Path

from simulator import main_generator


class TransactionContractTests(unittest.TestCase):
    def _schema_fields(self) -> dict[str, dict]:
        schema_path = Path("simulator/schemas/transaction.avsc")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        return {field["name"]: field for field in schema["fields"]}

    def test_amount_uses_decimal_logical_type(self) -> None:
        fields = self._schema_fields()

        amount_type = fields["amount"]["type"]
        self.assertEqual(amount_type["type"], "bytes")
        self.assertEqual(amount_type["logicalType"], "decimal")
        self.assertEqual(amount_type["precision"], 18)
        self.assertEqual(amount_type["scale"], 2)

    def test_event_time_epoch_us_is_required_long(self) -> None:
        fields = self._schema_fields()

        self.assertEqual(fields["event_time_epoch_us"]["type"], "long")
        self.assertNotIn("default", fields["event_time_epoch_us"])

    def test_timestamp_string_is_not_part_of_contract(self) -> None:
        fields = self._schema_fields()

        self.assertNotIn("timestamp", fields)

    def test_epoch_microseconds_preserves_subsecond_precision(self) -> None:
        event_time = datetime(2026, 5, 30, 12, 34, 56, 123456, tzinfo=UTC)

        self.assertEqual(main_generator.epoch_microseconds(event_time), 1_780_144_496_123_456)

    def test_epoch_microseconds_rejects_naive_datetime(self) -> None:
        event_time = datetime(2026, 5, 30, 12, 34, 56, 123456)

        with self.assertRaises(ValueError):
            main_generator.epoch_microseconds(event_time)

    def test_schema_contract_rejects_nullable_event_time(self) -> None:
        schema_path = Path("simulator/schemas/transaction.avsc")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        for field in schema["fields"]:
            if field["name"] == "event_time_epoch_us":
                field["type"] = ["null", "long"]
                field["default"] = None

        with self.assertRaisesRegex(ValueError, "event_time_epoch_us"):
            main_generator.validate_transaction_schema_contract(schema, schema_path)

    def test_schema_contract_rejects_redundant_timestamp(self) -> None:
        schema_path = Path("simulator/schemas/transaction.avsc")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        schema["fields"].insert(
            4,
            {
                "name": "timestamp",
                "type": "string",
            },
        )

        with self.assertRaisesRegex(ValueError, "timestamp"):
            main_generator.validate_transaction_schema_contract(schema, schema_path)

    def test_schema_registry_compatibility_is_normalized(self) -> None:
        self.assertEqual(
            main_generator.normalize_schema_registry_compatibility("backward_transitive"),
            "BACKWARD_TRANSITIVE",
        )

        with self.assertRaises(ValueError):
            main_generator.normalize_schema_registry_compatibility("loose")

    def test_schema_registry_compatibility_is_pinned_before_produce(self) -> None:
        schema_path = Path("simulator/schemas/transaction.avsc")
        schema_str = schema_path.read_text(encoding="utf-8")

        class FakeRegistryClient:
            def __init__(self) -> None:
                self.compatibility_call: tuple[str, str] | None = None

            def set_compatibility(self, *, subject_name: str, level: str) -> None:
                self.compatibility_call = (subject_name, level)

            def get_latest_version(self, subject_name: str) -> object:
                return object()

            def test_compatibility(
                self,
                subject_name: str,
                schema: object,
                version: str,
            ) -> bool:
                return subject_name == "financial.transactions-value" and version == "latest"

        client = FakeRegistryClient()

        main_generator.ensure_schema_registry_compatibility(
            schema_registry_client=client,
            subject="financial.transactions-value",
            compatibility="backward_transitive",
            schema_str=schema_str,
        )

        self.assertEqual(
            client.compatibility_call,
            ("financial.transactions-value", "BACKWARD_TRANSITIVE"),
        )


if __name__ == "__main__":
    unittest.main()
