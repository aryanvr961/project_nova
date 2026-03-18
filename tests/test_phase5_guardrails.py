import json
import os
import tempfile
import unittest

from phases import phase5_guardrails


class Phase5GuardrailsTests(unittest.TestCase):
    def _staged_record(self, **overrides) -> dict:
        record = {
            "cluster_id": "cluster_1",
            "cluster_uid": "nova_cluster_00001",
            "execution_status": "staged",
            "ready_for_guardrails": True,
            "transformation_type": "rule",
            "code": "lambda x: x",
            "confidence_score": 0.95,
            "reasoning": "Safe remediation candidate.",
            "fallback_value": "null",
            "inferred_anomaly_type": "type_cast",
            "model_used": "mock/static",
            "member_ids": ["row_1", "row_2"],
            "size": 2,
            "cache_hit": False,
            "pattern_key": "pattern_1",
            "guardrail_action": "auto_apply_with_audit",
            "risk_level": "low",
            "requires_human_review": False,
            "validation_checks": ["schema_check"],
            "affected_rows": 2,
            "unmatched_member_ids": [],
        }
        record.update(overrides)
        return record

    def test_approves_safe_record_for_promotion(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record()],
                    "staged_remediations": [self._staged_record()],
                    "anomalies": [{"id": 1}],
                    "phase1_summary": {"input_rows": 10, "anomalies": 1},
                    "phase4_summary": {"total": 1, "quarantined": 0, "execution_failed": 0},
                }
            )

            self.assertEqual(result["phase5_status"], "completed")
            self.assertEqual(result["phase5_summary"]["approved"], 1)
            self.assertEqual(result["phase5_summary"]["review_required"], 0)
            self.assertEqual(len(result["promotion_candidates"]), 1)
            self.assertTrue(os.path.exists(result["phase5_guardrail_file"]))

    def test_routes_medium_confidence_record_to_review(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record(confidence_score=0.82, risk_level="medium")],
                    "anomalies": [{"id": 1}, {"id": 2}, {"id": 3}],
                    "phase1_summary": {"input_rows": 20, "anomalies": 3},
                    "phase4_summary": {"total": 1, "quarantined": 0, "execution_failed": 0},
                }
            )

            self.assertEqual(result["phase5_summary"]["approved"], 0)
            self.assertEqual(result["phase5_summary"]["review_required"], 1)
            self.assertEqual(result["guardrail_review_required"][0]["guardrail_status"], "review_required")
            self.assertIn("confidence_below_promotion_threshold", result["guardrail_review_required"][0]["reasons"])

    def test_quarantines_high_risk_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record(risk_level="high")],
                    "anomalies": [{"id": 1}, {"id": 2}, {"id": 3}],
                    "phase1_summary": {"input_rows": 20, "anomalies": 3},
                    "phase4_summary": {"total": 1, "quarantined": 0, "execution_failed": 0},
                }
            )

            self.assertEqual(result["phase5_summary"]["quarantined"], 1)
            self.assertEqual(result["guardrail_quarantined"][0]["guardrail_status"], "quarantined")
            self.assertIn("high_risk_blocked", result["guardrail_quarantined"][0]["reasons"])

    def test_circuit_breaker_routes_staged_records_to_review(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record(), self._staged_record(cluster_id="cluster_2", cluster_uid="nova_cluster_00002")],
                    "anomalies": [{"id": i} for i in range(1, 9)],
                    "phase1_summary": {"input_rows": 10, "anomalies": 8},
                    "phase4_summary": {"total": 2, "quarantined": 1, "execution_failed": 0},
                }
            )

            self.assertTrue(result["phase5_summary"]["circuit_breaker_active"])
            self.assertEqual(result["phase5_summary"]["review_required"], 2)
            self.assertIn("bad_row_ratio_exceeded", result["phase5_summary"]["circuit_breaker_reasons"])

    def test_does_not_trigger_circuit_breaker_for_repeated_anomalies_on_same_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record()],
                    "anomalies": [
                        {"id": 1},
                        {"id": 1},
                        {"id": 1},
                        {"id": 2},
                        {"id": 2},
                    ],
                    "phase1_summary": {"input_rows": 10, "anomalies": 5},
                    "phase4_summary": {"total": 1, "quarantined": 0, "execution_failed": 0},
                }
            )

            self.assertFalse(result["phase5_summary"]["circuit_breaker_active"])
            self.assertEqual(result["phase5_summary"]["approved"], 1)

    def test_writes_guardrail_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase5_guardrails.run(
                {
                    "vault_dir": tmp_dir,
                    "execution_plan": [self._staged_record()],
                    "anomalies": [{"id": 1}],
                    "phase1_summary": {"input_rows": 10, "anomalies": 1},
                    "phase4_summary": {"total": 1, "quarantined": 0, "execution_failed": 0},
                }
            )

            with open(result["phase5_guardrail_file"], "r", encoding="utf-8") as handle:
                payload = json.load(handle)

            self.assertIn("phase5_policy", payload)
            self.assertIn("batch_risk_snapshot", payload)
            self.assertIn("guardrail_decisions", payload)
            self.assertIn("promotion_candidates", payload)


if __name__ == "__main__":
    unittest.main()
