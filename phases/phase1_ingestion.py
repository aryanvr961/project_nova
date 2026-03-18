"""
Module: PHASE 1 - DATA INGESTION
Owner: Aadyaa
Purpose:
- Load raw data and perform deterministic validation.
Responsibilities:
- Read local CSV/JSON input from context.
- Split rows into clean rows and anomaly rows.
- Preserve a downstream-safe context contract for Phase 2.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

NULL_LIKE_VALUES = {"", "null", "none", "na", "n/a", "nan", "missing", "unknown"}
DATE_COLUMN_HINTS = ("date", "time", "timestamp", "dob")
EMAIL_COLUMN_HINTS = ("email", "mail")
NUMERIC_COLUMN_HINTS = (
    "amount",
    "price",
    "cost",
    "total",
    "count",
    "qty",
    "quantity",
    "number",
    "score",
    "value",
    "rate",
    "percent",
    "balance",
)
DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
)
EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def _ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def _normalize_source_name(source_name: str, input_path: str, raw_dir: str) -> str:
    if source_name:
        return source_name
    if input_path:
        return Path(input_path).stem
    if raw_dir:
        return Path(raw_dir).name or "phase1_input"
    return "phase1_input"


def _resolve_input_path(context: dict[str, Any]) -> str:
    candidate_keys = ("input_path", "source_file", "dataset_path", "file_path")
    for key in candidate_keys:
        value = context.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    raw_dir = context.get("raw_dir")
    if not isinstance(raw_dir, str) or not raw_dir.strip():
        return ""

    raw_path = Path(raw_dir)
    if not raw_path.exists() or not raw_path.is_dir():
        return ""

    supported_files = sorted(
        path for path in raw_path.iterdir() if path.is_file() and path.suffix.lower() in {".csv", ".json", ".jsonl"}
    )
    if not supported_files:
        return ""
    return str(supported_files[0])


def _coerce_scalar(value: Any) -> Any:
    return value


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return [{key: _coerce_scalar(value) for key, value in row.items()} for row in reader]


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [{str(k): _coerce_scalar(v) for k, v in row.items()} for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        records = payload.get("records")
        if isinstance(records, list):
            return [{str(k): _coerce_scalar(v) for k, v in row.items()} for row in records if isinstance(row, dict)]
        return [{str(k): _coerce_scalar(v) for k, v in payload.items()}]
    raise ValueError(f"Unsupported JSON payload in {path}")


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            rows.append({str(k): _coerce_scalar(v) for k, v in obj.items()})
    return rows


def _load_rows(input_path: str) -> list[dict[str, Any]]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input dataset not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _load_csv_rows(path)
    if suffix == ".json":
        return _load_json_rows(path)
    if suffix == ".jsonl":
        return _load_jsonl_rows(path)
    raise ValueError(f"Unsupported input format: {path.suffix}")


def _is_null_like(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in NULL_LIKE_VALUES
    return False


def _looks_like_date_column(column_name: str) -> bool:
    normalized = column_name.lower()
    return any(token in normalized for token in DATE_COLUMN_HINTS)


def _looks_like_email_column(column_name: str) -> bool:
    normalized = column_name.lower()
    return any(token in normalized for token in EMAIL_COLUMN_HINTS)


def _looks_like_numeric_column(column_name: str) -> bool:
    normalized = column_name.lower()
    return any(token in normalized for token in NUMERIC_COLUMN_HINTS)


def _is_valid_date(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    for date_format in DATE_FORMATS:
        try:
            datetime.strptime(candidate, date_format)
            return True
        except ValueError:
            continue
    return False


def _is_valid_email(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if candidate != value:
        return False
    if ".." in candidate:
        return False
    if candidate.startswith(".") or candidate.endswith("."):
        return False
    return bool(EMAIL_PATTERN.match(candidate))


def _is_valid_numeric(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate or candidate != value:
        return False
    try:
        float(candidate)
        return True
    except ValueError:
        return False


def _make_anomaly(
    row: dict[str, Any],
    row_index: int,
    column_name: str,
    value: Any,
    error_type: str,
    anomaly_hint: str,
) -> dict[str, Any]:
    anomaly_id = row.get("id", row_index)
    return {
        "id": anomaly_id,
        "column_name": column_name,
        "value": value,
        "error_type": error_type,
        "anomaly_hint": anomaly_hint,
        "row_index": row_index,
        "source_row": dict(row),
    }


def _validate_row(row: dict[str, Any], row_index: int) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []

    for column_name, value in row.items():
        if _is_null_like(value):
            anomalies.append(
                _make_anomaly(
                    row,
                    row_index,
                    column_name,
                    value,
                    "null_block_error",
                    "value is missing or sentinel null",
                )
            )
            continue

        if _looks_like_date_column(column_name) and not _is_valid_date(value):
            anomalies.append(
                _make_anomaly(
                    row,
                    row_index,
                    column_name,
                    value,
                    "date_format_error",
                    "date token format mismatch",
                )
            )
            continue

        if _looks_like_email_column(column_name) and not _is_valid_email(value):
            anomalies.append(
                _make_anomaly(
                    row,
                    row_index,
                    column_name,
                    value,
                    "email_format_error",
                    "email token malformed address",
                )
            )
            continue

        if _looks_like_numeric_column(column_name) and not _is_valid_numeric(value):
            anomalies.append(
                _make_anomaly(
                    row,
                    row_index,
                    column_name,
                    value,
                    "numeric_format_error",
                    "amount contains numeric separator noise",
                )
            )

    return anomalies


def _write_clean_rows(path: str, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    _ensure_dir(str(output_path.parent))

    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_anomalies(path: str, anomalies: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    _ensure_dir(str(output_path.parent))
    output_path.write_text(
        json.dumps(anomalies, ensure_ascii=True, indent=2, default=str),
        encoding="utf-8",
    )


def save_pipeline_context(context: dict[str, Any], output_file: str | None = None) -> bool:
    vault_dir = context.get("vault_dir", os.path.join("data", "vault"))
    output_file = output_file or os.path.join(vault_dir, "phase1_context.json")
    _ensure_dir(os.path.dirname(output_file))
    with open(output_file, "w", encoding="utf-8") as handle:
        json.dump(context, handle, default=str, indent=2)
    return True


def _build_failure_result(
    context: dict[str, Any],
    *,
    phase1_status: str,
    message: str,
    validation_rules_applied: list[str],
) -> dict[str, Any]:
    updated_context = dict(context or {})
    updated_context.update(
        {
            "phase1_status": phase1_status,
            "phase1_summary": {
                "input_rows": 0,
                "clean_rows": 0,
                "anomalies": 0,
                "validation_rules_applied": validation_rules_applied,
                "message": message,
            },
            "raw_rows": [],
            "clean_rows": [],
            "anomalies": [],
            "phase1_clean_file": None,
            "phase1_anomaly_file": None,
        }
    )
    return updated_context


def run(context: dict) -> dict:
    """Run deterministic Phase 1 ingestion and return an updated context."""
    updated_context = dict(context or {})
    validation_rules_applied = [
        "null_detection",
        "date_format_detection",
        "email_format_detection",
        "numeric_format_detection",
    ]

    input_path = _resolve_input_path(updated_context)
    raw_dir = str(updated_context.get("raw_dir", os.path.join("data", "raw")))
    clean_dir = str(updated_context.get("clean_dir", os.path.join("data", "clean")))
    anomalies_dir = str(updated_context.get("anomalies_dir", os.path.join("data", "anomalies")))
    vault_dir = str(updated_context.get("vault_dir", os.path.join("data", "vault")))
    source_name = _normalize_source_name(str(updated_context.get("source_name", "")), input_path, raw_dir)

    if not input_path:
        return _build_failure_result(
            updated_context,
            phase1_status="missing_input",
            message="No local dataset path was provided.",
            validation_rules_applied=validation_rules_applied,
        )

    try:
        raw_rows = _load_rows(input_path)
    except Exception as exc:
        return _build_failure_result(
            updated_context,
            phase1_status="read_error",
            message=str(exc),
            validation_rules_applied=validation_rules_applied,
        )

    clean_rows: list[dict[str, Any]] = []
    anomalies: list[dict[str, Any]] = []

    for row_index, row in enumerate(raw_rows):
        row_copy = dict(row)
        row_copy.setdefault("row_index", row_index)
        row_anomalies = _validate_row(row_copy, row_index)
        if row_anomalies:
            anomalies.extend(row_anomalies)
        else:
            clean_rows.append(row_copy)

    _ensure_dir(clean_dir)
    _ensure_dir(anomalies_dir)
    _ensure_dir(vault_dir)

    clean_file = os.path.join(clean_dir, f"{source_name}_clean_rows.csv")
    anomaly_file = os.path.join(anomalies_dir, f"{source_name}_anomalies.json")
    _write_clean_rows(clean_file, clean_rows)
    _write_anomalies(anomaly_file, anomalies)

    updated_context.update(
        {
            "input_path": input_path,
            "source_name": source_name,
            "vault_dir": vault_dir,
            "raw_rows": raw_rows,
            "clean_rows": clean_rows,
            "anomalies": anomalies,
            "phase1_status": "completed" if raw_rows else "no_rows",
            "phase1_summary": {
                "input_rows": len(raw_rows),
                "clean_rows": len(clean_rows),
                "anomalies": len(anomalies),
                "validation_rules_applied": validation_rules_applied,
            },
            "phase1_clean_file": clean_file,
            "phase1_anomaly_file": anomaly_file,
        }
    )

    save_pipeline_context(updated_context)
    return updated_context


def ingest_and_validate(
    raw_dir: str,
    clean_dir: str,
    anomalies_dir: str,
    schema_path: str,
    source_name: str = "source",
    pk_columns: list[str] | None = None,
    required_columns: list[str] | None = None,
    vault_dir: str = os.path.join("data", "vault"),
) -> dict[str, Any]:
    context = {
        "raw_dir": raw_dir,
        "clean_dir": clean_dir,
        "anomalies_dir": anomalies_dir,
        "schema_path": schema_path,
        "source_name": source_name,
        "pk_columns": pk_columns or [],
        "required_columns": required_columns or [],
        "vault_dir": vault_dir,
    }
    return run(context)
