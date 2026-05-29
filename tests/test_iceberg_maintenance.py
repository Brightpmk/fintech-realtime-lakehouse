import os
import unittest
from unittest.mock import patch

from storage import iceberg_maintenance


class IcebergMaintenanceTests(unittest.TestCase):
    def test_validate_table_name_accepts_fully_qualified_table(self) -> None:
        table_name = "iceberg.silver.transactions"
        self.assertEqual(iceberg_maintenance.validate_table_name(table_name), table_name)

    def test_validate_table_name_rejects_unsafe_table(self) -> None:
        for table_name in (
            "silver.transactions",
            "iceberg.silver.transactions.extra",
            "iceberg.silver.transactions;drop",
            "iceberg.silver.bad-name",
        ):
            with self.subTest(table_name=table_name):
                with self.assertRaises(ValueError):
                    iceberg_maintenance.validate_table_name(table_name)

    def test_parse_tables_from_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ICEBERG_MAINTENANCE_TABLES": (
                    "iceberg.silver.transactions, iceberg.gold.gold_hourly_liquidity"
                )
            },
            clear=True,
        ):
            tables = iceberg_maintenance.parse_tables()

        self.assertEqual(
            tables,
            ["iceberg.silver.transactions", "iceberg.gold.gold_hourly_liquidity"],
        )

    def test_parse_tables_rejects_empty_env(self) -> None:
        with patch.dict(os.environ, {"ICEBERG_MAINTENANCE_TABLES": " , "}, clear=True):
            with self.assertRaises(ValueError):
                iceberg_maintenance.parse_tables()


if __name__ == "__main__":
    unittest.main()
