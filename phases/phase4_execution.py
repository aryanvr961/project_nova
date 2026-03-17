"""
Module: PHASE 4 - EXECUTION ENGINE
Owner: Aadyaa
Purpose:
- Validate and stage Phase 3 remediation outputs.
Responsibilities:
- Check remediation payload contract.
- Prepare execution plan metadata for downstream guardrails.
- Route invalid remediations to quarantine-style staging.
"""

from __future__ import annotations

from typing import Any


REQUIRED_REMEDIATION_FIELDS = {
    "cluster_id",
    "transformation_type",
    "code",
    "confidence_score",
    "reasoning",
    "fallback_value",
    "inferred_anomaly_type",
    "model_used",
    "member_ids",
    "size",
    "cache_hit",
    "pattern_key",
}


def _validate_remediation(remediation: dict[str, Any]) -> tuple[bool, list[str]]:
    missing = sorted(REQUIRED_REMEDIATION_FIELDS - remediation.keys())
    return (not missing, missing)


def _build_execution_record(remediation: dict[str, Any]) -> dict[str, Any]:
    is_valid, missing = _validate_remediation(remediation)
    if not is_valid:
        return {
            "cluster_id": remediation.get("cluster_id", "unknown"),
            "execution_status": "invalid_contract",
            "ready_for_guardrails": False,
            "missing_fields": missing,
            "transformation_type": "quarantine",
        }

    transformation_type = remediation["transformation_type"]
    execution_status = "staged"
    if transformation_type == "quarantine":
        execution_status = "quarantined"

    return {
        "cluster_id": remediation["cluster_id"],
        "execution_status": execution_status,
        "ready_for_guardrails": execution_status == "staged",
        "transformation_type": transformation_type,
        "code": remediation["code"],
        "confidence_score": remediation["confidence_score"],
        "reasoning": remediation["reasoning"],
        "fallback_value": remediation["fallback_value"],
        "member_ids": remediation["member_ids"],
        "size": remediation["size"],
        "pattern_key": remediation["pattern_key"],
    }


def run(context: dict) -> dict:
    """Consume Phase 3 remediations and stage execution metadata."""
    print("[Phase 4] Starting execution staging")
    updated_context = dict(context or {})
    remediations = updated_context.get("remediations", [])

    if not isinstance(remediations, list) or not remediations:
        updated_context["execution_plan"] = []
        updated_context["phase4_status"] = "no_remediations"
        updated_context["phase4_summary"] = {
            "total": 0,
            "staged": 0,
            "quarantined": 0,
            "invalid_contract": 0,
        }
        return updated_context

    execution_plan: list[dict[str, Any]] = []
    staged = 0
    quarantined = 0
    invalid_contract = 0

    for remediation in remediations:
        if not isinstance(remediation, dict):
            invalid_contract += 1
            execution_plan.append(
                {
                    "cluster_id": "unknown",
                    "execution_status": "invalid_contract",
                    "ready_for_guardrails": False,
                    "missing_fields": ["<non-dict remediation>"],
                    "transformation_type": "quarantine",
                }
            )
            continue

        record = _build_execution_record(remediation)
        status = record["execution_status"]
        if status == "staged":
            staged += 1
        elif status == "quarantined":
            quarantined += 1
        else:
            invalid_contract += 1
        execution_plan.append(record)

    updated_context["execution_plan"] = execution_plan
    updated_context["phase4_status"] = "completed"
    updated_context["phase4_summary"] = {
        "total": len(execution_plan),
        "staged": staged,
        "quarantined": quarantined,
        "invalid_contract": invalid_contract,
    }
    return updated_context
