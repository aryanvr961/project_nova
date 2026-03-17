import unittest

from phases import phase4_execution


class Phase4ExecutionTests(unittest.TestCase):
    def test_stages_valid_remediation(self):
        context = {
            "remediations": [
                {
                    "cluster_id": "cluster_1",
                    "transformation_type": "rule",
                    "code": "lambda x: x",
                    "confidence_score": 0.82,
                    "reasoning": "Conservative rule selected.",
                    "fallback_value": "null",
                    "inferred_anomaly_type": "date_format",
                    "model_used": "mock/static",
                    "member_ids": ["row_1"],
                    "size": 1,
                    "cache_hit": False,
                    "pattern_key": "date-pattern",
                }
            ]
        }

        result = phase4_execution.run(context)

        self.assertEqual(result["phase4_status"], "completed")
        self.assertEqual(result["phase4_summary"]["staged"], 1)
        self.assertEqual(result["execution_plan"][0]["execution_status"], "staged")

    def test_flags_invalid_contract(self):
        context = {
            "remediations": [
                {
                    "cluster_id": "cluster_2",
                    "transformation_type": "rule",
                }
            ]
        }

        result = phase4_execution.run(context)

        self.assertEqual(result["phase4_summary"]["invalid_contract"], 1)
        self.assertEqual(result["execution_plan"][0]["execution_status"], "invalid_contract")


if __name__ == "__main__":
    unittest.main()
