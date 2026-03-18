import json
import tempfile
import unittest
from pathlib import Path

from phases import phase1_ingestion as phase1


class Phase1IngestionTests(unittest.TestCase):
    def test_missing_input_file_returns_error_context(self):
        result = phase1.run({"input_path": "does_not_exist.csv"})

        self.assertEqual(result["phase1_status"], "error")
        self.assertEqual(result["anomalies"], [])
        self.assertEqual(result["clean_rows"], [])

    def test_raw_rows_input_splits_clean_and_anomalies(self):
        rows = [
            {"id": 1, "transaction_date": "2024-03-12", "customer_email": "alice@example.com", "amount": "120.50"},
            {"id": 2, "transaction_date": "12/03/2024", "customer_email": "badmail", "amount": "12,0x"},
        ]

        result = phase1.run({"raw_rows": rows, "required_columns": ["transaction_date", "customer_email", "amount"]})

        self.assertEqual(result["phase1_status"], "completed")
        self.assertEqual(result["phase1_summary"]["input_rows"], 2)
        self.assertEqual(result["phase1_summary"]["clean_rows"], 1)
        self.assertGreaterEqual(result["phase1_summary"]["anomalies"], 2)

    def test_csv_input_detects_nulls(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sample.csv"
            path.write_text("id,name,email\n1,Alice,alice@example.com\n2,,bob@example.com\n", encoding="utf-8")

            result = phase1.run({"input_path": str(path), "required_columns": ["name", "email"]})

        self.assertEqual(result["phase1_status"], "completed")
        self.assertEqual(result["phase1_summary"]["clean_rows"], 1)
        self.assertEqual(result["phase1_summary"]["anomalies"], 1)
        self.assertEqual(result["anomalies"][0]["error_type"], "null_block_error")

    def test_anomaly_shape_matches_phase2_expectation(self):
        rows = [{"id": 10, "transaction_date": "March 12, 2024", "customer_email": "broken@", "amount": "100"}]

        result = phase1.run({"raw_rows": rows})

        anomaly = result["anomalies"][0]
        self.assertIn("id", anomaly)
        self.assertIn("column_name", anomaly)
        self.assertIn("value", anomaly)
        self.assertIn("error_type", anomaly)
        self.assertIn("anomaly_hint", anomaly)
        self.assertIn("source_phase", anomaly)

    def test_json_input_supported(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sample.json"
            payload = [
                {"id": 1, "transaction_date": "2024-03-12", "customer_email": "alice@example.com", "amount": "5.5"},
                {"id": 2, "transaction_date": "2024/03/12", "customer_email": "bob@example.com", "amount": "5.5"},
            ]
            path.write_text(json.dumps(payload), encoding="utf-8")

            result = phase1.run({"input_path": str(path)})

        self.assertEqual(result["phase1_status"], "completed")
        self.assertEqual(result["phase1_summary"]["input_rows"], 2)
        self.assertEqual(result["phase1_summary"]["anomalies"], 1)

    def test_schema_driven_duplicate_detection(self):
        rows = [
            {"id": 1, "customer_email": "alice@example.com", "amount": "5.5"},
            {"id": 2, "customer_email": "alice@example.com", "amount": "8.1"},
        ]
        schema = {
            "columns": {
                "customer_email": {"type": "email", "required": True, "unique": True},
                "amount": {"type": "numeric", "required": True},
            }
        }

        result = phase1.run({"raw_rows": rows, "validation_schema": schema})

        self.assertEqual(result["phase1_status"], "completed")
        self.assertEqual(result["phase1_summary"]["clean_rows"], 0)
        self.assertEqual(result["phase1_summary"]["anomaly_breakdown"]["duplicate_value_error"], 2)
        self.assertTrue(result["phase1_audit_file"].endswith("phase1_audit_summary.json"))

    def test_schema_driven_required_columns_work_without_context_override(self):
        rows = [{"id": 1, "customer_email": "alice@example.com"}]
        schema = {
            "columns": {
                "customer_email": {"type": "email", "required": True},
                "amount": {"type": "numeric", "required": True},
            }
        }

        result = phase1.run({"raw_rows": rows, "validation_schema": schema})

        self.assertEqual(result["phase1_status"], "completed")
        self.assertEqual(result["phase1_summary"]["anomalies"], 1)
        self.assertIn("amount", result["phase1_summary"]["required_columns"])


if __name__ == "__main__":
    unittest.main()
