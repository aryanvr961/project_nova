"""
Module: PHASE 1 - DATA INGESTION
Owner: Aadyaa
Purpose:
- Load raw data and perform deterministic validation.
Responsibilities:
- Read files from local input sources.
- Perform rule-based validation.
- Split dataset into clean and anomaly datasets.
- Produce context that downstream phases can consume directly.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any


DATA_DIR = Path("data")
CLEAN_DIR = DATA_DIR / "clean"
ANOMALY_DIR = DATA_DIR / "anomalies"
VAULT_DIR = DATA_DIR / "vault"
SUPPORTED_SUFFIXES = {".csv", ".json", ".jsonl"}

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
NUMERIC_PATTERN = re.compile(r"^-?\d+(?:\.\d+)?$")
DATE_PATTERNS = (
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    re.compile(r"^\d{2}/\d{2}/\d{4}$"),
    re.compile(r"^\d{2}-\d{2}-\d{4}$"),
)
NULL_LIKE_VALUES = {"", "null", "none", "nan", "n/a", "na"}


def _normalize_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    return Path(path_value)


def _load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return [dict(row) for row in csv.DictReader(file)]


def _load_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("records"), list):
            return [item for item in payload["records"] if isinstance(item, dict)]
        return [payload]
    return []


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_rows_from_path(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _load_csv(path)
    if suffix == ".json":
        return _load_json(path)
    if suffix == ".jsonl":
        return _load_jsonl(path)
    raise ValueError(f"Unsupported input format: {path.suffix}")


def _load_input_rows(context: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    raw_rows = context.get("raw_rows")
    if isinstance(raw_rows, list):
        return [row for row in raw_rows if isinstance(row, dict)], None

    source_path = _normalize_path(
        context.get("input_path") or context.get("source_file") or context.get("dataset_path")
    )
    if source_path is None:
        return [], None
    if not source_path.exists():
        raise FileNotFoundError(f"Input file not found: {source_path}")
    if source_path.suffix.lower() not in SUPPORTED_SUFFIXES:
        raise ValueError(f"Unsupported input format: {source_path.suffix}")
    return _load_rows_from_path(source_path), str(source_path)


def _load_validation_schema(context: dict[str, Any]) -> dict[str, Any]:
    schema = context.get("validation_schema")
    if isinstance(schema, dict):
        return schema

    schema_path = _normalize_path(context.get("schema_path"))
    if schema_path is not None and schema_path.exists():
        payload = json.loads(schema_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    return {}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text.lower() in NULL_LIKE_VALUES


def _looks_like_email(column_name: str) -> bool:
    name = column_name.lower()
    return "email" in name


def _looks_like_date(column_name: str) -> bool:
    name = column_name.lower()
    return "date" in name or "dob" in name or "timestamp" in name


def _looks_like_numeric(column_name: str) -> bool:
    name = column_name.lower()
    return any(token in name for token in ("amount", "price", "cost", "total", "qty", "quantity", "num", "score"))


def _is_valid_date(text: str) -> bool:
    value = text.strip()
    return any(pattern.match(value) for pattern in DATE_PATTERNS)


def _is_valid_email(text: str) -> bool:
    return bool(EMAIL_PATTERN.match(text.strip()))


def _is_valid_numeric(text: str) -> bool:
    normalized = text.strip().replace(",", "")
    return bool(NUMERIC_PATTERN.match(normalized))


def _rule_names_from_schema(schema: dict[str, Any]) -> list[str]:
    rules = {"required", "email", "date", "numeric"}
    unique_columns = schema.get("unique_columns")
    if isinstance(unique_columns, list) and unique_columns:
        rules.add("duplicate")
    return sorted(rules)


def _make_anomaly(
    *,
    row: dict[str, Any],
    row_id: Any,
    column_name: str,
    value: Any,
    error_type: str,
    anomaly_hint: str,
) -> dict[str, Any]:
    return {
        "id": row_id,
        "column_name": column_name,
        "value": value,
        "error_type": error_type,
        "anomaly_hint": anomaly_hint,
        "source_phase": "phase1_ingestion",
        "source_row": row,
    }


def _row_identifier(index: int, row: dict[str, Any]) -> Any:
    for key in ("id", "row_id", "record_id"):
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return index + 1


def _column_type(column_name: str, schema: dict[str, Any]) -> str | None:
    columns = schema.get("columns")
    if isinstance(columns, dict):
        entry = columns.get(column_name)
        if isinstance(entry, dict):
            column_type = entry.get("type")
            if isinstance(column_type, str) and column_type.strip():
                return column_type.strip().lower()
    return None


def _required_columns(context: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    required_columns = context.get("required_columns")
    if isinstance(required_columns, list):
        return [str(column) for column in required_columns]

    columns = schema.get("columns")
    if isinstance(columns, dict):
        return [
            str(column_name)
            for column_name, entry in columns.items()
            if isinstance(entry, dict) and entry.get("required") is True
        ]
    return []


def _unique_columns(schema: dict[str, Any]) -> list[str]:
    unique_columns = schema.get("unique_columns")
    if isinstance(unique_columns, list):
        return [str(column) for column in unique_columns]

    columns = schema.get("columns")
    if isinstance(columns, dict):
        return [
            str(column_name)
            for column_name, entry in columns.items()
            if isinstance(entry, dict) and entry.get("unique") is True
        ]
    return []


def _expected_type(column_name: str, schema: dict[str, Any]) -> str | None:
    declared_type = _column_type(column_name, schema)
    if declared_type:
        return declared_type
    if _looks_like_email(column_name):
        return "email"
    if _looks_like_date(column_name):
        return "date"
    if _looks_like_numeric(column_name):
        return "numeric"
    return None


def _type_error_payload(expected_type: str) -> tuple[str, str]:
    if expected_type == "email":
        return "email_format_error", "email token malformed address"
    if expected_type == "date":
        return "date_format_error", "date token format mismatch"
    if expected_type == "numeric":
        return "numeric_format_error", "amount contains numeric separator noise"
    return "type_validation_error", f"value does not match expected {expected_type} format"


def _matches_expected_type(value: str, expected_type: str) -> bool:
    if expected_type == "email":
        return _is_valid_email(value)
    if expected_type == "date":
        return _is_valid_date(value)
    if expected_type == "numeric":
        return _is_valid_numeric(value)
    return True


def _find_duplicate_columns(rows: list[dict[str, Any]], unique_columns: list[str]) -> dict[int, list[str]]:
    duplicates_by_row: dict[int, list[str]] = {}
    for column_name in unique_columns:
        seen: dict[str, list[int]] = {}
        for index, row in enumerate(rows):
            value = row.get(column_name)
            if _is_missing(value):
                continue
            key = str(value).strip().lower()
            seen.setdefault(key, []).append(index)
        for indexes in seen.values():
            if len(indexes) < 2:
                continue
            for index in indexes:
                duplicates_by_row.setdefault(index, []).append(column_name)
    return duplicates_by_row


def _validate_row(
    index: int,
    row: dict[str, Any],
    required_columns: list[str],
    schema: dict[str, Any],
    duplicate_columns: list[str],
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    row_id = _row_identifier(index, row)

    columns_to_check = set(row.keys()) | set(required_columns)
    for column_name in sorted(columns_to_check):
        value = row.get(column_name)
        if column_name in required_columns and _is_missing(value):
            anomalies.append(
                _make_anomaly(
                    row=row,
                    row_id=row_id,
                    column_name=column_name,
                    value=value,
                    error_type="null_block_error",
                    anomaly_hint="value is missing or sentinel null",
                )
            )
            continue

        if _is_missing(value):
            continue

        text = str(value).strip()
        expected_type = _expected_type(column_name, schema)
        if expected_type is not None and not _matches_expected_type(text, expected_type):
            error_type, anomaly_hint = _type_error_payload(expected_type)
            anomalies.append(
                _make_anomaly(
                    row=row,
                    row_id=row_id,
                    column_name=column_name,
                    value=value,
                    error_type=error_type,
                    anomaly_hint=anomaly_hint,
                )
            )

    for column_name in sorted(set(duplicate_columns)):
        anomalies.append(
            _make_anomaly(
                row=row,
                row_id=row_id,
                column_name=column_name,
                value=row.get(column_name),
                error_type="duplicate_value_error",
                anomaly_hint="duplicate value detected in unique column",
            )
        )
    return anomalies


def _anomaly_breakdown(anomalies: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for anomaly in anomalies:
        error_type = str(anomaly.get("error_type", "unknown"))
        counts[error_type] = counts.get(error_type, 0) + 1
    return counts


def _persist_audit_summary(summary: dict[str, Any]) -> str:
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    path = VAULT_DIR / "phase1_audit_summary.json"
    _write_json(path, summary)
    return str(path)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str), encoding="utf-8")


def run(context: dict) -> dict:
    """Run deterministic ingestion and return Phase 1 context for downstream phases."""
    print("[Phase 1] Starting ingestion")
    updated_context = dict(context or {})

    schema = _load_validation_schema(updated_context)
    validation_rules = _rule_names_from_schema(schema)

    try:
        rows, loaded_from = _load_input_rows(updated_context)
    except Exception as exc:
        updated_context["raw_rows"] = []
        updated_context["clean_rows"] = []
        updated_context["anomalies"] = []
        updated_context["phase1_status"] = "error"
        updated_context["phase1_summary"] = {
            "input_rows": 0,
            "clean_rows": 0,
            "anomalies": 0,
            "validation_rules_applied": validation_rules,
            "anomaly_breakdown": {},
        }
        updated_context["phase1_error"] = str(exc)
        return updated_context

    if not rows:
        updated_context["raw_rows"] = []
        updated_context["clean_rows"] = []
        updated_context["anomalies"] = []
        updated_context["phase1_status"] = "no_input"
        updated_context["phase1_summary"] = {
            "input_rows": 0,
            "clean_rows": 0,
            "anomalies": 0,
            "validation_rules_applied": validation_rules,
            "anomaly_breakdown": {},
        }
        if loaded_from is not None:
            updated_context["phase1_input_path"] = loaded_from
        return updated_context

    required_columns = _required_columns(updated_context, schema)
    duplicate_map = _find_duplicate_columns(rows, _unique_columns(schema))

    clean_rows: list[dict[str, Any]] = []
    anomalies: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        row_anomalies = _validate_row(index, row, required_columns, schema, duplicate_map.get(index, []))
        if row_anomalies:
            anomalies.extend(row_anomalies)
        else:
            clean_rows.append(row)

    anomaly_breakdown = _anomaly_breakdown(anomalies)
    audit_summary = {
        "phase": "phase1_ingestion",
        "input_rows": len(rows),
        "clean_rows": len(clean_rows),
        "anomalies": len(anomalies),
        "anomaly_breakdown": anomaly_breakdown,
        "required_columns": required_columns,
        "unique_columns": _unique_columns(schema),
        "loaded_from": loaded_from,
    }

    updated_context["raw_rows"] = rows
    updated_context["clean_rows"] = clean_rows
    updated_context["anomalies"] = anomalies
    updated_context["phase1_status"] = "completed"
    updated_context["phase1_summary"] = {
        "input_rows": len(rows),
        "clean_rows": len(clean_rows),
        "anomalies": len(anomalies),
        "validation_rules_applied": validation_rules,
        "anomaly_breakdown": anomaly_breakdown,
        "required_columns": required_columns,
    }
    if loaded_from is not None:
        updated_context["phase1_input_path"] = loaded_from

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    ANOMALY_DIR.mkdir(parents=True, exist_ok=True)
    clean_path = CLEAN_DIR / "phase1_clean_rows.json"
    anomaly_path = ANOMALY_DIR / "phase1_anomalies.json"
    _write_json(clean_path, clean_rows)
    _write_json(anomaly_path, anomalies)
    updated_context["phase1_clean_file"] = str(clean_path)
    updated_context["phase1_anomaly_file"] = str(anomaly_path)
    updated_context["phase1_audit_file"] = _persist_audit_summary(audit_summary)

    print(f"[Phase 1] Completed: {len(rows)} rows -> {len(clean_rows)} clean, {len(anomalies)} anomalies")
    return updated_context
