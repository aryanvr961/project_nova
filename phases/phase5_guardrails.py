"""
Module: PHASE 5 - SAFETY GUARDRAILS
Owner: Aryan
Purpose:
- Prevent unsafe AI operations from progressing toward promotion.
Responsibilities:
- Validate staged execution outputs against policy thresholds.
- Route risky records to quarantine or human review.
- Trigger a circuit breaker when batch-level risk exceeds safe limits.
- Persist an audit-friendly guardrail decision artifact for Phase 6.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


DEFAULT_POLICY = {
    "min_confidence_for_review_pass": 0.75,
    "min_confidence_for_promotion": 0.9,
    "max_quarantine_ratio": 0.4,
    "max_execution_failure_ratio": 0.15,
    "max_unmatched_member_ratio": 0.2,
    "max_bad_row_ratio": 0.6,
    "block_high_risk": True,
}


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _write_guardrail_artifact(path: str, payload: dict[str, Any]) -> str:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)
    return path


def _load_policy(context: dict[str, Any]) -> dict[str, Any]:
    policy = dict(DEFAULT_POLICY)
    overrides = context.get("phase5_policy")
    if isinstance(overrides, dict):
        policy.update(overrides)
    return policy


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _batch_risk_snapshot(context: dict[str, Any]) -> dict[str, Any]:
    phase1_summary = context.get("phase1_summary", {})
    phase4_summary = context.get("phase4_summary", {})
    anomalies = context.get("anomalies", [])

    input_rows = int(phase1_summary.get("input_rows", 0) or 0)
    anomaly_count = int(phase1_summary.get("anomalies", 0) or 0)
    total_execution = int(phase4_summary.get("total", 0) or 0)
    quarantined = int(phase4_summary.get("quarantined", 0) or 0)
    execution_failed = int(phase4_summary.get("execution_failed", 0) or 0)
    unique_bad_rows = 0
    if isinstance(anomalies, list):
        unique_bad_rows = len(
            {
                str(anomaly.get("id"))
                for anomaly in anomalies
                if isinstance(anomaly, dict) and anomaly.get("id") is not None
            }
        )

    return {
        "input_rows": input_rows,
        "anomalies": anomaly_count,
        "unique_bad_rows": unique_bad_rows,
        "bad_row_ratio": _safe_ratio(unique_bad_rows, input_rows),
        "execution_total": total_execution,
        "quarantine_ratio": _safe_ratio(quarantined, total_execution),
        "execution_failure_ratio": _safe_ratio(execution_failed, total_execution),
    }


def _circuit_breaker(snapshot: dict[str, Any], policy: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if snapshot["quarantine_ratio"] > float(policy["max_quarantine_ratio"]):
        reasons.append("quarantine_ratio_exceeded")
    if snapshot["execution_failure_ratio"] > float(policy["max_execution_failure_ratio"]):
        reasons.append("execution_failure_ratio_exceeded")
    if snapshot["bad_row_ratio"] > float(policy["max_bad_row_ratio"]):
        reasons.append("bad_row_ratio_exceeded")
    return (bool(reasons), reasons)


def _decision_base(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "cluster_id": record.get("cluster_id", "unknown"),
        "cluster_uid": record.get("cluster_uid", "unknown"),
        "execution_status": record.get("execution_status", "unknown"),
        "guardrail_status": "pending",
        "promotion_ready": False,
        "requires_human_review": bool(record.get("requires_human_review", False)),
        "risk_level": str(record.get("risk_level", "unknown")),
        "guardrail_action": str(record.get("guardrail_action", "unknown")),
        "confidence_score": float(record.get("confidence_score", 0.0) or 0.0),
        "affected_rows": int(record.get("affected_rows", 0) or 0),
        "reasons": [],
        "unmatched_member_ids": list(record.get("unmatched_member_ids", [])),
        "validation_checks": list(record.get("validation_checks", [])),
    }


def _review_decision(record: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    decision = _decision_base(record)
    decision["guardrail_status"] = "review_required"
    decision["requires_human_review"] = True
    decision["reasons"] = reasons
    return decision


def _quarantine_decision(record: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    decision = _decision_base(record)
    decision["guardrail_status"] = "quarantined"
    decision["requires_human_review"] = True
    decision["reasons"] = reasons
    return decision


def _approved_decision(record: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    decision = _decision_base(record)
    decision["guardrail_status"] = "approved"
    decision["promotion_ready"] = True
    decision["reasons"] = reasons
    return decision


def _evaluate_record(record: dict[str, Any], policy: dict[str, Any], *, circuit_breaker_active: bool) -> dict[str, Any]:
    if circuit_breaker_active:
        return _review_decision(record, ["circuit_breaker_active"])

    execution_status = str(record.get("execution_status", "unknown"))
    confidence_score = float(record.get("confidence_score", 0.0) or 0.0)
    risk_level = str(record.get("risk_level", "unknown")).lower()
    requires_human_review = bool(record.get("requires_human_review", False))
    member_ids = list(record.get("member_ids", []))
    unmatched_member_ids = list(record.get("unmatched_member_ids", []))
    unmatched_ratio = _safe_ratio(len(unmatched_member_ids), len(member_ids))
    affected_rows = int(record.get("affected_rows", 0) or 0)

    if execution_status in {"invalid_contract", "execution_failed", "quarantined"}:
        return _quarantine_decision(record, [f"execution_status={execution_status}"])

    if execution_status != "staged":
        return _quarantine_decision(record, ["unknown_execution_state"])

    if affected_rows <= 0:
        return _quarantine_decision(record, ["no_rows_affected"])

    if unmatched_ratio > float(policy["max_unmatched_member_ratio"]):
        return _quarantine_decision(record, ["unmatched_member_ratio_exceeded"])

    if policy.get("block_high_risk", True) and risk_level == "high":
        return _quarantine_decision(record, ["high_risk_blocked"])

    if confidence_score < float(policy["min_confidence_for_review_pass"]):
        return _quarantine_decision(record, ["confidence_below_guardrail_floor"])

    if requires_human_review:
        return _review_decision(record, ["human_review_required"])

    if confidence_score < float(policy["min_confidence_for_promotion"]):
        return _review_decision(record, ["confidence_below_promotion_threshold"])

    return _approved_decision(record, ["all_guardrail_checks_passed"])


def run(context: dict) -> dict:
    """Evaluate staged execution outputs and produce promotion-ready guardrail decisions."""
    print("[Phase 5] Starting guardrails checks")
    updated_context = dict(context or {})
    vault_dir = str(updated_context.get("vault_dir", os.path.join("data", "vault")))
    execution_plan = updated_context.get("execution_plan", [])
    staged_remediations = updated_context.get("staged_remediations", [])
    quarantined_remediations = updated_context.get("quarantined_remediations", [])
    policy = _load_policy(updated_context)

    if not isinstance(execution_plan, list) or not execution_plan:
        updated_context["guardrail_decisions"] = []
        updated_context["guardrail_approved"] = []
        updated_context["guardrail_review_required"] = []
        updated_context["guardrail_quarantined"] = list(quarantined_remediations) if isinstance(quarantined_remediations, list) else []
        updated_context["promotion_candidates"] = []
        updated_context["phase5_status"] = "no_execution_plan"
        updated_context["phase5_summary"] = {
            "total": 0,
            "approved": 0,
            "review_required": 0,
            "quarantined": len(updated_context["guardrail_quarantined"]),
            "circuit_breaker_active": False,
            "circuit_breaker_reasons": [],
        }
        updated_context["phase5_guardrail_file"] = None
        return updated_context

    snapshot = _batch_risk_snapshot(updated_context)
    circuit_breaker_active, circuit_breaker_reasons = _circuit_breaker(snapshot, policy)

    decisions: list[dict[str, Any]] = []
    approved: list[dict[str, Any]] = []
    review_required: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []

    for record in execution_plan:
        if not isinstance(record, dict):
            decision = _quarantine_decision({}, ["non_dict_execution_record"])
        else:
            decision = _evaluate_record(record, policy, circuit_breaker_active=circuit_breaker_active)
            decision["cluster_payload"] = record

        decisions.append(decision)
        if decision["guardrail_status"] == "approved":
            approved.append(decision)
        elif decision["guardrail_status"] == "review_required":
            review_required.append(decision)
        else:
            quarantined.append(decision)

    promotion_candidates = [decision["cluster_payload"] for decision in approved if "cluster_payload" in decision]

    artifact_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase5_policy": policy,
        "batch_risk_snapshot": snapshot,
        "phase5_summary": {
            "total": len(decisions),
            "approved": len(approved),
            "review_required": len(review_required),
            "quarantined": len(quarantined),
            "circuit_breaker_active": circuit_breaker_active,
            "circuit_breaker_reasons": circuit_breaker_reasons,
        },
        "guardrail_decisions": decisions,
        "promotion_candidates": promotion_candidates,
        "incoming_staged_remediations": staged_remediations if isinstance(staged_remediations, list) else [],
        "incoming_quarantined_remediations": quarantined_remediations if isinstance(quarantined_remediations, list) else [],
    }
    guardrail_file = _write_guardrail_artifact(
        os.path.join(vault_dir, "phase5_guardrail_report.json"),
        artifact_payload,
    )

    updated_context["vault_dir"] = vault_dir
    updated_context["guardrail_decisions"] = decisions
    updated_context["guardrail_approved"] = approved
    updated_context["guardrail_review_required"] = review_required
    updated_context["guardrail_quarantined"] = quarantined
    updated_context["promotion_candidates"] = promotion_candidates
    updated_context["phase5_status"] = "completed"
    updated_context["phase5_summary"] = artifact_payload["phase5_summary"]
    updated_context["phase5_guardrail_file"] = guardrail_file
    return updated_context
