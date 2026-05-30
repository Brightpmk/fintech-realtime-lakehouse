import json
import unittest
from pathlib import Path


class TransactionContractTests(unittest.TestCase):
    def test_amount_uses_decimal_logical_type(self) -> None:
        schema_path = Path("simulator/schemas/transaction.avsc")
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        fields = {field["name"]: field for field in schema["fields"]}

        amount_type = fields["amount"]["type"]
        self.assertEqual(amount_type["type"], "bytes")
        self.assertEqual(amount_type["logicalType"], "decimal")
        self.assertEqual(amount_type["precision"], 18)
        self.assertEqual(amount_type["scale"], 2)


if __name__ == "__main__":
    unittest.main()
