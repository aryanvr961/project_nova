"""
Module: PHASE 6 - PRODUCTION PROMOTION
Owner: Aryan
Purpose:
- Promote only guardrail-approved staging outputs into a production-ready payload.
Responsibilities:
- Validate final promotion preconditions.
- Block unsafe batches when review/quarantine gates remain unresolved.
- Build promoted row payloads from staged execution rows.
- Persist a final audit artifact for downstream storage or handoff.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any


DEFAULT_PROMOTION_POLICY = {
    "require_zero_review_items": True,
    "require_zero_quarantined_items": True,
    "require_phase5_completed": True,
    "require_guardrail_artifact": True,
}


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _write_promotion_artifact(path: str, payload: dict[str, Any]) -> str:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)
    return path


def _load_policy(context: dict[str, Any]) -> dict[str, Any]:
    policy = dict(DEFAULT_PROMOTION_POLICY)
    overrides = context.get("phase6_policy")
    if isinstance(overrides, dict):
        policy.update(overrides)
    return policy


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_str_set(values: list[Any], key: str) -> set[str]:
    result: set[str] = set()
    for item in values:
        if isinstance(item, dict):
            value = item.get(key)
            if value is not None:
                result.add(str(value))
    return result


def _build_promoted_rows(
    promotion_candidates: list[dict[str, Any]],
    staged_execution_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    approved_cluster_uids = _safe_str_set(promotion_candidates, "cluster_uid")
    promoted_rows: list[dict[str, Any]] = []

    for row in staged_execution_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("cluster_uid", "")) not in approved_cluster_uids:
            continue
        promoted_rows.append(
            {
                "member_id": row.get("member_id"),
                "cluster_id": row.get("cluster_id"),
                "cluster_uid": row.get("cluster_uid"),
                "target_column": row.get("target_column"),
                "promoted_value": row.get("transformed_value"),
                "source_row_after": row.get("source_row_after"),
                "promotion_status": "promoted",
            }
        )
    return promoted_rows


def _promotion_blockers(context: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    phase5_status = str(context.get("phase5_status", ""))
    review_required = _safe_list(context.get("guardrail_review_required"))
    quarantined = _safe_list(context.get("guardrail_quarantined"))
    approved = _safe_list(context.get("guardrail_approved"))
    promotion_candidates = _safe_list(context.get("promotion_candidates"))
    guardrail_file = context.get("phase5_guardrail_file")

    if policy.get("require_phase5_completed", True) and phase5_status != "completed":
        blockers.append("phase5_not_completed")
    if policy.get("require_guardrail_artifact", True) and not guardrail_file:
        blockers.append("missing_phase5_guardrail_artifact")
    if policy.get("require_zero_review_items", True) and review_required:
        blockers.append("guardrail_review_pending")
    if policy.get("require_zero_quarantined_items", True) and quarantined:
        blockers.append("guardrail_quarantine_pending")
    if approved and not promotion_candidates:
        blockers.append("approved_without_promotion_candidates")
    return blockers


def run(context: dict) -> dict:
    """Promote only approved rows into a production-ready payload and final audit artifact."""
    print("[Phase 6] Starting promotion flow")
    updated_context = dict(context or {})
    vault_dir = str(updated_context.get("vault_dir", os.path.join("data", "vault")))
    policy = _load_policy(updated_context)

    promotion_candidates = _safe_list(updated_context.get("promotion_candidates"))
    guardrail_approved = _safe_list(updated_context.get("guardrail_approved"))
    guardrail_review_required = _safe_list(updated_context.get("guardrail_review_required"))
    guardrail_quarantined = _safe_list(updated_context.get("guardrail_quarantined"))
    staged_execution_rows = _safe_list(updated_context.get("staged_execution_rows"))

    blockers = _promotion_blockers(updated_context, policy)
    promoted_rows: list[dict[str, Any]] = []
    promoted_clusters: list[dict[str, Any]] = []

    if not blockers and promotion_candidates:
        promoted_rows = _build_promoted_rows(promotion_candidates, staged_execution_rows)
        approved_cluster_uids = _safe_str_set(promotion_candidates, "cluster_uid")
        for candidate in promotion_candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("cluster_uid", "")) in approved_cluster_uids:
                promoted_clusters.append(
                    {
                        "cluster_id": candidate.get("cluster_id"),
                        "cluster_uid": candidate.get("cluster_uid"),
                        "promotion_status": "promoted",
                        "affected_rows": int(candidate.get("affected_rows", 0) or 0),
                        "confidence_score": float(candidate.get("confidence_score", 0.0) or 0.0),
                    }
                )

    phase6_status = "completed" if not blockers else "blocked"
    artifact_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase6_policy": policy,
        "phase6_summary": {
            "approved_inputs": len(guardrail_approved),
            "review_pending": len(guardrail_review_required),
            "quarantined_pending": len(guardrail_quarantined),
            "promotion_candidates": len(promotion_candidates),
            "promoted_clusters": len(promoted_clusters),
            "promoted_rows": len(promoted_rows),
            "promotion_blocked": bool(blockers),
            "blockers": blockers,
        },
        "promoted_clusters": promoted_clusters,
        "promoted_rows": promoted_rows,
        "promotion_candidates": promotion_candidates,
    }
    promotion_file = _write_promotion_artifact(
        os.path.join(vault_dir, "phase6_promotion_report.json"),
        artifact_payload,
    )

    updated_context["vault_dir"] = vault_dir
    updated_context["production_payload"] = promoted_rows
    updated_context["promoted_clusters"] = promoted_clusters
    updated_context["phase6_status"] = phase6_status
    updated_context["phase6_summary"] = artifact_payload["phase6_summary"]
    updated_context["phase6_promotion_file"] = promotion_file
    return updated_context
