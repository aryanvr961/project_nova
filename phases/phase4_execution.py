"""
Module: PHASE 4 - EXECUTION ENGINE
Owner: Aadyaa
Purpose:
- Validate and stage Phase 3 remediation outputs.
Responsibilities:
- Check remediation payload contract.
- Safely apply remediation lambdas to anomaly-linked rows.
- Prepare execution metadata for downstream guardrails.
- Persist an audit-friendly staging artifact for Phase 5 and later phases.
"""

from __future__ import annotations

import ast
import json
import math
import os
from datetime import datetime, timezone
from typing import Any


REQUIRED_REMEDIATION_FIELDS = {
    "cluster_id",
    "cluster_uid",
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
    "guardrail_action",
    "risk_level",
    "requires_human_review",
    "validation_checks",
}

SAFE_GLOBALS = {
    "__builtins__": {},
    "str": str,
    "int": int,
    "float": float,
    "len": len,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
}
ALLOWED_AST_NODES = {
    ast.Expression,
    ast.Lambda,
    ast.arguments,
    ast.arg,
    ast.Load,
    ast.Name,
    ast.Constant,
    ast.IfExp,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Is,
    ast.IsNot,
    ast.In,
    ast.NotIn,
    ast.And,
    ast.Or,
    ast.BoolOp,
    ast.UnaryOp,
    ast.Not,
    ast.USub,
    ast.UAdd,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.Mod,
    ast.Pow,
    ast.Call,
    ast.Attribute,
    ast.Subscript,
    ast.Index,
    ast.Slice,
    ast.List,
    ast.Tuple,
    ast.Dict,
}
ALLOWED_CALL_NAMES = {"str", "int", "float", "len", "min", "max", "abs", "round"}
ALLOWED_STRING_METHODS = {"replace", "strip", "lower", "upper"}


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _validate_remediation(remediation: dict[str, Any]) -> tuple[bool, list[str]]:
    missing = sorted(REQUIRED_REMEDIATION_FIELDS - remediation.keys())
    return (not missing, missing)


def _build_invalid_record(remediation: dict[str, Any] | None, missing_fields: list[str]) -> dict[str, Any]:
    remediation = remediation or {}
    return {
        "cluster_id": remediation.get("cluster_id", "unknown"),
        "cluster_uid": remediation.get("cluster_uid", "unknown"),
        "execution_status": "invalid_contract",
        "ready_for_guardrails": False,
        "missing_fields": missing_fields,
        "transformation_type": "quarantine",
        "guardrail_action": "quarantine",
        "risk_level": "high",
        "requires_human_review": True,
        "validation_checks": remediation.get("validation_checks", []),
    }


def _build_execution_record(remediation: dict[str, Any]) -> dict[str, Any]:
    is_valid, missing = _validate_remediation(remediation)
    if not is_valid:
        return _build_invalid_record(remediation, missing)

    transformation_type = remediation["transformation_type"]
    guardrail_action = remediation["guardrail_action"]
    execution_status = "staged"
    if transformation_type == "quarantine" or guardrail_action == "quarantine":
        execution_status = "quarantined"

    return {
        "cluster_id": remediation["cluster_id"],
        "cluster_uid": remediation["cluster_uid"],
        "execution_status": execution_status,
        "ready_for_guardrails": execution_status == "staged",
        "transformation_type": transformation_type,
        "code": remediation["code"],
        "confidence_score": remediation["confidence_score"],
        "reasoning": remediation["reasoning"],
        "fallback_value": remediation["fallback_value"],
        "inferred_anomaly_type": remediation["inferred_anomaly_type"],
        "model_used": remediation["model_used"],
        "member_ids": remediation["member_ids"],
        "size": remediation["size"],
        "cache_hit": remediation["cache_hit"],
        "pattern_key": remediation["pattern_key"],
        "guardrail_action": guardrail_action,
        "risk_level": remediation["risk_level"],
        "requires_human_review": remediation["requires_human_review"],
        "validation_checks": remediation["validation_checks"],
        "raw_response": remediation.get("raw_response"),
    }


def _validate_lambda_ast(code: str) -> None:
    tree = ast.parse(code, mode="eval")
    for node in ast.walk(tree):
        if type(node) not in ALLOWED_AST_NODES:
            raise ValueError(f"Disallowed syntax in remediation code: {type(node).__name__}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id not in ALLOWED_CALL_NAMES:
                    raise ValueError("Only simple safe built-in calls are allowed in remediation code")
            elif isinstance(node.func, ast.Attribute):
                if node.func.attr not in ALLOWED_STRING_METHODS:
                    raise ValueError("Only safe string cleanup methods are allowed in remediation code")
            else:
                raise ValueError("Unsupported callable in remediation code")
    if not isinstance(tree.body, ast.Lambda):
        raise ValueError("Remediation code must be a lambda expression")


def _compile_lambda(code: str):
    _validate_lambda_ast(code)
    compiled = eval(code, SAFE_GLOBALS, {})
    if not callable(compiled):
        raise ValueError("Remediation code did not compile to a callable lambda")
    return compiled


def _normalize_value(value: Any) -> Any:
    if value in {"null", "None", "none", "NULL", "N/A", "n/a", ""}:
        return None
    return value


def _safe_json_value(value: Any) -> Any:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _remediation_id_candidates(member_id: str) -> list[str]:
    candidates = [member_id]
    if member_id.startswith("row_"):
        candidates.append(member_id[4:])
    return candidates


def _build_anomaly_index(anomalies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for anomaly in anomalies:
        if not isinstance(anomaly, dict):
            continue
        anomaly_id = anomaly.get("id")
        if anomaly_id is not None:
            index[str(anomaly_id)] = anomaly
            index[f"row_{anomaly_id}"] = anomaly
    return index


def _apply_to_anomaly_rows(remediation: dict[str, Any], anomaly_index: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    transformed_rows: list[dict[str, Any]] = []
    unmatched_member_ids: list[str] = []
    transformer = _compile_lambda(str(remediation["code"]))

    for member_id in remediation["member_ids"]:
        matched_row = None
        for candidate in _remediation_id_candidates(str(member_id)):
            matched_row = anomaly_index.get(candidate)
            if matched_row is not None:
                break

        if matched_row is None:
            unmatched_member_ids.append(str(member_id))
            continue

        original_value = matched_row.get("value")
        source_row = matched_row.get("source_row")
        target_column = matched_row.get("column_name") or matched_row.get("target_column")
        transformed_value = transformer(_normalize_value(original_value))
        transformed_value = _safe_json_value(transformed_value)

        updated_source_row = None
        if isinstance(source_row, dict):
            updated_source_row = dict(source_row)
            if target_column:
                updated_source_row[str(target_column)] = transformed_value

        transformed_rows.append(
            {
                "member_id": str(member_id),
                "cluster_id": remediation["cluster_id"],
                "cluster_uid": remediation["cluster_uid"],
                "target_column": target_column,
                "original_value": original_value,
                "transformed_value": transformed_value,
                "fallback_value": remediation["fallback_value"],
                "source_row_before": source_row,
                "source_row_after": updated_source_row,
                "apply_status": "applied",
            }
        )

    return transformed_rows, unmatched_member_ids


def _stage_execution_rows(
    record: dict[str, Any],
    anomaly_index: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
    if record["execution_status"] != "staged":
        return [], record, None

    try:
        transformed_rows, unmatched_member_ids = _apply_to_anomaly_rows(record, anomaly_index)
    except Exception as exc:
        failure_record = dict(record)
        failure_record["execution_status"] = "execution_failed"
        failure_record["ready_for_guardrails"] = False
        failure_record["guardrail_action"] = "quarantine"
        failure_record["risk_level"] = "high"
        failure_record["requires_human_review"] = True
        failure_record["execution_error"] = str(exc)
        return [], record, failure_record

    enriched_record = dict(record)
    enriched_record["affected_rows"] = len(transformed_rows)
    enriched_record["unmatched_member_ids"] = unmatched_member_ids
    return transformed_rows, enriched_record, None


def _write_execution_artifact(path: str, payload: dict[str, Any]) -> str:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=True, default=str)
    return path


def run(context: dict) -> dict:
    """Consume Phase 3 remediations and stage execution metadata."""
    print("[Phase 4] Starting execution staging")
    updated_context = dict(context or {})
    remediations = updated_context.get("remediations", [])
    anomalies = updated_context.get("anomalies", [])
    vault_dir = str(updated_context.get("vault_dir", os.path.join("data", "vault")))

    if not isinstance(remediations, list) or not remediations:
        updated_context["execution_plan"] = []
        updated_context["staged_remediations"] = []
        updated_context["quarantined_remediations"] = []
        updated_context["staged_execution_rows"] = []
        updated_context["phase4_status"] = "no_remediations"
        updated_context["phase4_summary"] = {
            "total": 0,
            "staged": 0,
            "quarantined": 0,
            "invalid_contract": 0,
            "execution_failed": 0,
            "applied_rows": 0,
        }
        updated_context["phase4_execution_file"] = None
        return updated_context

    anomaly_index = _build_anomaly_index(anomalies if isinstance(anomalies, list) else [])
    execution_plan: list[dict[str, Any]] = []
    staged_remediations: list[dict[str, Any]] = []
    quarantined_remediations: list[dict[str, Any]] = []
    staged_execution_rows: list[dict[str, Any]] = []
    staged = 0
    quarantined = 0
    invalid_contract = 0
    execution_failed = 0

    for remediation in remediations:
        if not isinstance(remediation, dict):
            record = _build_invalid_record(None, ["<non-dict remediation>"])
            invalid_contract += 1
            quarantined_remediations.append(record)
            execution_plan.append(record)
            continue

        base_record = _build_execution_record(remediation)
        status = base_record["execution_status"]
        if status == "invalid_contract":
            invalid_contract += 1
            quarantined_remediations.append(base_record)
            execution_plan.append(base_record)
            continue

        transformed_rows, enriched_record, execution_failure = _stage_execution_rows(base_record, anomaly_index)
        if execution_failure is not None:
            execution_failed += 1
            quarantined += 1
            quarantined_remediations.append(execution_failure)
            execution_plan.append(execution_failure)
            continue

        if status == "staged":
            staged += 1
            staged_remediations.append(enriched_record)
            staged_execution_rows.extend(transformed_rows)
        elif status == "quarantined":
            quarantined += 1
            quarantined_remediations.append(base_record)
        execution_plan.append(enriched_record if status == "staged" else base_record)

    artifact_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase4_summary": {
            "total": len(execution_plan),
            "staged": staged,
            "quarantined": quarantined,
            "invalid_contract": invalid_contract,
            "execution_failed": execution_failed,
            "applied_rows": len(staged_execution_rows),
        },
        "execution_plan": execution_plan,
        "staged_remediations": staged_remediations,
        "quarantined_remediations": quarantined_remediations,
        "staged_execution_rows": staged_execution_rows,
    }
    execution_file = _write_execution_artifact(
        os.path.join(vault_dir, "phase4_execution_plan.json"),
        artifact_payload,
    )

    updated_context["vault_dir"] = vault_dir
    updated_context["execution_plan"] = execution_plan
    updated_context["staged_remediations"] = staged_remediations
    updated_context["quarantined_remediations"] = quarantined_remediations
    updated_context["staged_execution_rows"] = staged_execution_rows
    updated_context["phase4_status"] = "completed"
    updated_context["phase4_summary"] = artifact_payload["phase4_summary"]
    updated_context["phase4_execution_file"] = execution_file
    return updated_context
