import importlib
import os
import unittest


os.environ["PHASE3_PROVIDER"] = "mock"

from phases import phase3_slm_remediation as phase3


class Phase3RemediationTests(unittest.TestCase):
    def setUp(self):
        importlib.reload(phase3)

    def test_run_returns_remediations_for_clusters(self):
        context = {
            "clusters": [
                {
                    "cluster_id": "cluster_1",
                    "sample_rows": [
                        {"transaction_date": "15-03-2024"},
                        {"transaction_date": "22/01/2024"},
                    ],
                    "size": 2,
                    "member_ids": ["row_1", "row_2"],
                    "cache_hit": False,
                    "pattern_key": "date-pattern",
                }
            ]
        }

        result = phase3.run(context)

        self.assertEqual(result["phase3_status"], "completed")
        self.assertEqual(result["phase3_summary"]["total"], 1)
        self.assertEqual(result["phase3_summary"]["provider"], "mock")
        self.assertEqual(len(result["remediations"]), 1)
        self.assertEqual(result["remediations"][0]["cluster_id"], "cluster_1")
        self.assertIn("reasoning", result["remediations"][0])

    def test_ambiguous_cluster_is_quarantined(self):
        context = {
            "clusters": [
                {
                    "cluster_id": "cluster_2",
                    "sample_rows": [
                        {"comment": "abc###"},
                        {"comment": "???"}
                    ],
                    "size": 2,
                    "member_ids": ["row_3", "row_4"],
                    "cache_hit": True,
                    "pattern_key": "ambiguous-pattern",
                }
            ]
        }

        result = phase3.run(context)

        self.assertEqual(result["phase3_summary"]["quarantined"], 1)
        self.assertEqual(result["remediations"][0]["transformation_type"], "quarantine")

    def test_retrieval_context_returns_rules(self):
        cluster = phase3._normalize_cluster(
            {
                "cluster_id": "cluster_3",
                "sample_rows": [{"amount": "1,200.50"}],
                "size": 1,
                "member_ids": ["row_5"],
                "cache_hit": False,
                "pattern_key": "numeric-format",
            }
        )

        self.assertTrue(cluster.rule_context["retrieved_rules"])

    def test_normalize_cluster_prefers_value_field(self):
        cluster = phase3._normalize_cluster(
            {
                "cluster_id": "cluster_4",
                "sample_rows": [
                    {
                        "id": 101,
                        "column_name": "transaction_date",
                        "value": "03-12-2024",
                        "error_type": "date_format_error",
                    }
                ],
                "size": 1,
                "member_ids": ["row_6"],
                "cache_hit": False,
                "pattern_key": "date-value-field",
            }
        )

        self.assertEqual(cluster.target_column, "transaction_date")
        self.assertEqual(cluster.sample_values, ["03-12-2024"])
        self.assertEqual(cluster.inferred_anomaly_type, "date_format")

    def test_no_clusters_short_circuits(self):
        result = phase3.run({})

        self.assertEqual(result["phase3_status"], "no_clusters")
        self.assertEqual(result["phase3_summary"]["total"], 0)
        self.assertEqual(result["remediations"], [])


if __name__ == "__main__":
    unittest.main()
