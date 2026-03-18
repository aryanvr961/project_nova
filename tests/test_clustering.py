import unittest

from phases import phase1_ingestion as phase1
from phases import phase2_clustering as phase2


class Phase2ClusteringTests(unittest.TestCase):
    def test_phase1_anomalies_include_unique_anomaly_uid(self):
        result = phase1.run(
            {
                "raw_rows": [
                    {
                        "id": 1,
                        "transaction_date": "March 12 2024",
                        "customer_email": "badmail",
                        "amount": "12,0x",
                    }
                ],
                "required_columns": ["transaction_date", "customer_email", "amount"],
            }
        )

        anomaly_uids = {anomaly.get("anomaly_uid") for anomaly in result["anomalies"]}
        self.assertEqual(len(anomaly_uids), 3)
        self.assertNotIn(None, anomaly_uids)

    def test_phase2_handles_multiple_anomalies_from_same_source_row(self):
        phase1_result = phase1.run(
            {
                "raw_rows": [
                    {
                        "id": 1,
                        "transaction_date": "March 12 2024",
                        "customer_email": "badmail",
                        "amount": "12,0x",
                    },
                    {
                        "id": 2,
                        "transaction_date": "2024-03-12",
                        "customer_email": "also-bad",
                        "amount": "120.50",
                    },
                ],
                "required_columns": ["transaction_date", "customer_email", "amount"],
            }
        )

        phase2_result = phase2.run({"anomalies": phase1_result["anomalies"]})

        self.assertEqual(phase2_result["phase2_status"], "completed")
        self.assertEqual(phase2_result["phase2_metrics"]["input_anomalies"], len(phase1_result["anomalies"]))
        member_ids = [member_id for cluster in phase2_result["clusters"] for member_id in cluster["member_ids"]]
        self.assertEqual(len(member_ids), len(set(member_ids)))


if __name__ == "__main__":
    unittest.main()
