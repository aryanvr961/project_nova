import json
import os
import tempfile
import unittest

from phases import phase3_slm_remediation as phase3
from phases import phase4_execution


class Phase4ExecutionTests(unittest.TestCase):
    def _valid_remediation(self) -> dict:
        return {
            "cluster_id": "cluster_1",
            "cluster_uid": "nova_cluster_00001",
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
            "guardrail_action": "staging_only",
            "risk_level": "medium",
            "requires_human_review": False,
            "validation_checks": ["schema_check", "null_safety_check"],
            "raw_response": '{"transformation_type":"rule"}',
        }

    def _anomaly_rows(self) -> list[dict]:
        return [
            {
                "id": 1,
                "anomaly_uid": "1:amount:numeric_format_error",
                "column_name": "amount",
                "value": "1,200.50",
                "error_type": "numeric_format_error",
                "anomaly_hint": "amount contains numeric separator noise",
                "source_row": {
                    "id": 1,
                    "amount": "1,200.50",
                    "customer_email": "nova@example.com",
                },
            }
        ]

    def test_stages_valid_remediation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            context = {
                "vault_dir": tmp_dir,
                "remediations": [self._valid_remediation()],
            }

            result = phase4_execution.run(context)

            self.assertEqual(result["phase4_status"], "completed")
            self.assertEqual(result["phase4_summary"]["staged"], 1)
            self.assertEqual(result["execution_plan"][0]["execution_status"], "staged")
            self.assertEqual(result["execution_plan"][0]["cluster_uid"], "nova_cluster_00001")
            self.assertEqual(result["execution_plan"][0]["guardrail_action"], "staging_only")
            self.assertTrue(os.path.exists(result["phase4_execution_file"]))

    def test_applies_safe_lambda_to_anomaly_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            remediation = self._valid_remediation()
            remediation["code"] = "lambda x: None if x is None else str(x).replace(',', '')"
            remediation["member_ids"] = ["row_1"]
            remediation["inferred_anomaly_type"] = "type_cast"

            result = phase4_execution.run(
                {
                    "vault_dir": tmp_dir,
                    "remediations": [remediation],
                    "anomalies": self._anomaly_rows(),
                }
            )

            self.assertEqual(result["phase4_summary"]["applied_rows"], 1)
            self.assertEqual(len(result["staged_execution_rows"]), 1)
            self.assertEqual(result["staged_execution_rows"][0]["transformed_value"], "1200.50")
            self.assertEqual(result["staged_execution_rows"][0]["source_row_after"]["amount"], "1200.50")

    def test_applies_when_member_id_uses_anomaly_uid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            remediation = self._valid_remediation()
            remediation["code"] = "lambda x: None if x is None else str(x).replace(',', '')"
            remediation["member_ids"] = ["anomaly_1:amount:numeric_format_error"]
            remediation["inferred_anomaly_type"] = "type_cast"

            result = phase4_execution.run(
                {
                    "vault_dir": tmp_dir,
                    "remediations": [remediation],
                    "anomalies": self._anomaly_rows(),
                }
            )

            self.assertEqual(result["phase4_summary"]["applied_rows"], 1)
            self.assertEqual(result["staged_execution_rows"][0]["member_id"], "anomaly_1:amount:numeric_format_error")
            self.assertEqual(result["staged_execution_rows"][0]["transformed_value"], "1200.50")

    def test_quarantine_remediation_is_split_out(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            remediation = self._valid_remediation()
            remediation["transformation_type"] = "quarantine"
            remediation["guardrail_action"] = "quarantine"
            remediation["requires_human_review"] = True

            result = phase4_execution.run({"vault_dir": tmp_dir, "remediations": [remediation]})

            self.assertEqual(result["phase4_summary"]["quarantined"], 1)
            self.assertEqual(len(result["staged_remediations"]), 0)
            self.assertEqual(len(result["quarantined_remediations"]), 1)
            self.assertEqual(result["quarantined_remediations"][0]["execution_status"], "quarantined")

    def test_execution_failure_quarantines_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            remediation = self._valid_remediation()
            remediation["code"] = "lambda x: __import__('os').system('echo nope')"

            result = phase4_execution.run(
                {
                    "vault_dir": tmp_dir,
                    "remediations": [remediation],
                    "anomalies": self._anomaly_rows(),
                }
            )

            self.assertEqual(result["phase4_summary"]["execution_failed"], 1)
            self.assertEqual(result["quarantined_remediations"][0]["execution_status"], "execution_failed")
            self.assertIn("execution_error", result["quarantined_remediations"][0])

    def test_flags_invalid_contract(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            context = {
                "vault_dir": tmp_dir,
                "remediations": [
                    {
                        "cluster_id": "cluster_2",
                        "transformation_type": "rule",
                    }
                ],
            }

            result = phase4_execution.run(context)

            self.assertEqual(result["phase4_summary"]["invalid_contract"], 1)
            self.assertEqual(result["execution_plan"][0]["execution_status"], "invalid_contract")
            self.assertIn("cluster_uid", result["execution_plan"][0]["missing_fields"])

    def test_writes_execution_artifact_payload(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase4_execution.run({"vault_dir": tmp_dir, "remediations": [self._valid_remediation()]})

            with open(result["phase4_execution_file"], "r", encoding="utf-8") as handle:
                payload = json.load(handle)

            self.assertIn("execution_plan", payload)
            self.assertIn("staged_remediations", payload)
            self.assertIn("quarantined_remediations", payload)
            self.assertIn("staged_execution_rows", payload)
            self.assertEqual(payload["phase4_summary"]["staged"], 1)

    def test_phase3_output_flows_into_phase4(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.environ["PHASE3_PROVIDER"] = "mock"
            phase3_result = phase3.run(
                {
                    "clusters": [
                        {
                            "cluster_id": "cluster_4",
                            "cluster_uid": "nova_cluster_00004",
                            "sample_rows": [{"amount": "1,200.50"}],
                            "size": 1,
                            "member_ids": ["row_4"],
                            "cache_hit": False,
                            "pattern_key": "numeric-format",
                            "target_column": "amount",
                        }
                    ]
                }
            )

            phase4_result = phase4_execution.run({"vault_dir": tmp_dir, **phase3_result})

            self.assertEqual(phase4_result["phase4_status"], "completed")
            self.assertEqual(phase4_result["phase4_summary"]["total"], 1)
            self.assertIn(phase4_result["execution_plan"][0]["execution_status"], {"staged", "quarantined", "execution_failed"})
            self.assertIn("guardrail_action", phase4_result["execution_plan"][0])


if __name__ == "__main__":
    unittest.main()
