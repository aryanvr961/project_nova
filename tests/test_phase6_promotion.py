import json
import os
import tempfile
import unittest

from phases import phase6_promotion


class Phase6PromotionTests(unittest.TestCase):
    def _approved_candidate(self, **overrides) -> dict:
        candidate = {
            "cluster_id": "cluster_1",
            "cluster_uid": "nova_cluster_00001",
            "execution_status": "staged",
            "confidence_score": 0.95,
            "affected_rows": 2,
        }
        candidate.update(overrides)
        return candidate

    def _staged_rows(self) -> list[dict]:
        return [
            {
                "member_id": "row_1",
                "cluster_id": "cluster_1",
                "cluster_uid": "nova_cluster_00001",
                "target_column": "amount",
                "transformed_value": "1200.50",
                "source_row_after": {"id": 1, "amount": "1200.50"},
            },
            {
                "member_id": "row_2",
                "cluster_id": "cluster_1",
                "cluster_uid": "nova_cluster_00001",
                "target_column": "amount",
                "transformed_value": "999.00",
                "source_row_after": {"id": 2, "amount": "999.00"},
            },
        ]

    def test_promotes_approved_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase6_promotion.run(
                {
                    "vault_dir": tmp_dir,
                    "phase5_status": "completed",
                    "phase5_guardrail_file": os.path.join(tmp_dir, "phase5_guardrail_report.json"),
                    "guardrail_approved": [{"cluster_uid": "nova_cluster_00001"}],
                    "guardrail_review_required": [],
                    "guardrail_quarantined": [],
                    "promotion_candidates": [self._approved_candidate()],
                    "staged_execution_rows": self._staged_rows(),
                }
            )

            self.assertEqual(result["phase6_status"], "completed")
            self.assertEqual(result["phase6_summary"]["promoted_rows"], 2)
            self.assertEqual(result["phase6_summary"]["promoted_clusters"], 1)
            self.assertEqual(len(result["production_payload"]), 2)
            self.assertTrue(os.path.exists(result["phase6_promotion_file"]))

    def test_blocks_when_review_items_exist(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase6_promotion.run(
                {
                    "vault_dir": tmp_dir,
                    "phase5_status": "completed",
                    "phase5_guardrail_file": os.path.join(tmp_dir, "phase5_guardrail_report.json"),
                    "guardrail_approved": [{"cluster_uid": "nova_cluster_00001"}],
                    "guardrail_review_required": [{"cluster_uid": "nova_cluster_00002"}],
                    "guardrail_quarantined": [],
                    "promotion_candidates": [self._approved_candidate()],
                    "staged_execution_rows": self._staged_rows(),
                }
            )

            self.assertEqual(result["phase6_status"], "blocked")
            self.assertIn("guardrail_review_pending", result["phase6_summary"]["blockers"])
            self.assertEqual(result["phase6_summary"]["promoted_rows"], 0)

    def test_blocks_when_quarantine_items_exist(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase6_promotion.run(
                {
                    "vault_dir": tmp_dir,
                    "phase5_status": "completed",
                    "phase5_guardrail_file": os.path.join(tmp_dir, "phase5_guardrail_report.json"),
                    "guardrail_approved": [{"cluster_uid": "nova_cluster_00001"}],
                    "guardrail_review_required": [],
                    "guardrail_quarantined": [{"cluster_uid": "nova_cluster_00003"}],
                    "promotion_candidates": [self._approved_candidate()],
                    "staged_execution_rows": self._staged_rows(),
                }
            )

            self.assertEqual(result["phase6_status"], "blocked")
            self.assertIn("guardrail_quarantine_pending", result["phase6_summary"]["blockers"])

    def test_blocks_when_phase5_not_completed(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase6_promotion.run(
                {
                    "vault_dir": tmp_dir,
                    "phase5_status": "placeholder",
                    "guardrail_approved": [],
                    "guardrail_review_required": [],
                    "guardrail_quarantined": [],
                    "promotion_candidates": [],
                    "staged_execution_rows": [],
                }
            )

            self.assertEqual(result["phase6_status"], "blocked")
            self.assertIn("phase5_not_completed", result["phase6_summary"]["blockers"])

    def test_writes_promotion_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = phase6_promotion.run(
                {
                    "vault_dir": tmp_dir,
                    "phase5_status": "completed",
                    "phase5_guardrail_file": os.path.join(tmp_dir, "phase5_guardrail_report.json"),
                    "guardrail_approved": [{"cluster_uid": "nova_cluster_00001"}],
                    "guardrail_review_required": [],
                    "guardrail_quarantined": [],
                    "promotion_candidates": [self._approved_candidate()],
                    "staged_execution_rows": self._staged_rows(),
                }
            )

            with open(result["phase6_promotion_file"], "r", encoding="utf-8") as handle:
                payload = json.load(handle)

            self.assertIn("phase6_policy", payload)
            self.assertIn("phase6_summary", payload)
            self.assertIn("promoted_clusters", payload)
            self.assertIn("promoted_rows", payload)


if __name__ == "__main__":
    unittest.main()
