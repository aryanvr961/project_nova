import importlib
import json
import os
import tempfile
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

    def test_fast_path_handles_type_cast_without_provider_call(self):
        cluster = phase3._normalize_cluster(
            {
                "cluster_id": "cluster_5",
                "sample_rows": [{"amount": "1,200.50"}],
                "size": 1,
                "member_ids": ["row_7"],
                "cache_hit": False,
                "pattern_key": "type-cast-fast-path",
                "target_column": "amount",
            }
        )

        result = phase3._remediate_cluster(cluster)

        self.assertEqual(result.model_used, "deterministic/type_cast")
        self.assertEqual(result.transformation_type, "lambda")
        self.assertIn("replace", result.code)

    def test_pattern_cache_reuses_historical_remediation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_memory_file = phase3.REMEDIATION_MEMORY_FILE
            phase3.REMEDIATION_MEMORY_FILE = phase3.Path(tmp_dir) / "remediation_memory.jsonl"
            phase3.REMEDIATION_MEMORY_FILE.write_text(
                json.dumps(
                    {
                        "memory_id": "rem_1",
                        "pattern_key": "cached-pattern",
                        "target_column": "amount",
                        "inferred_anomaly_type": "type_cast",
                        "transformation_type": "lambda",
                        "code": "lambda x: None if x is None else str(x).replace(',', '')",
                        "confidence_score": 0.95,
                        "reasoning": "Cached remediation reused.",
                        "fallback_value": "null",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            try:
                cluster = phase3._normalize_cluster(
                    {
                        "cluster_id": "cluster_6",
                        "sample_rows": [{"amount": "7,500.00"}],
                        "size": 1,
                        "member_ids": ["row_8"],
                        "cache_hit": True,
                        "pattern_key": "cached-pattern",
                        "target_column": "amount",
                    }
                )

                result = phase3._remediate_cluster(cluster)
            finally:
                phase3.REMEDIATION_MEMORY_FILE = original_memory_file

        self.assertEqual(result.model_used, "cache/remediation_memory")
        self.assertEqual(result.transformation_type, "lambda")
        self.assertGreaterEqual(result.confidence_score, 0.95)

    def test_no_clusters_short_circuits(self):
        result = phase3.run({})

        self.assertEqual(result["phase3_status"], "no_clusters")
        self.assertEqual(result["phase3_summary"]["total"], 0)
        self.assertEqual(result["remediations"], [])


if __name__ == "__main__":
    unittest.main()
