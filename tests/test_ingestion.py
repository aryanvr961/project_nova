"""
Module: TEST - INGESTION
Owner: Aadyaa
Purpose:
- Test deterministic Phase 1 ingestion behavior.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phases.phase1_ingestion import run


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _base_context(tmp_path: Path, input_path: Path | None = None) -> dict:
    context = {
        "clean_dir": str(tmp_path / "data" / "clean"),
        "anomalies_dir": str(tmp_path / "data" / "anomalies"),
        "vault_dir": str(tmp_path / "data" / "vault"),
        "source_name": "phase1_test",
    }
    if input_path is not None:
        context["input_path"] = str(input_path)
    return context


def test_run_phase1_handles_missing_input_gracefully(tmp_path: Path) -> None:
    result = run(_base_context(tmp_path))

    assert result["phase1_status"] == "missing_input"
    assert result["clean_rows"] == []
    assert result["anomalies"] == []
    assert result["phase1_summary"]["input_rows"] == 0


def test_run_phase1_passes_clean_rows_through(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "clean.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "101",
                "transaction_date": "2024-03-12",
                "customer_email": "nova@example.com",
                "amount": "125.40",
                "status": "ok",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    assert result["phase1_status"] == "completed"
    assert result["phase1_summary"]["input_rows"] == 1
    assert result["phase1_summary"]["clean_rows"] == 1
    assert result["phase1_summary"]["anomalies"] == 0
    assert len(result["clean_rows"]) == 1
    assert result["anomalies"] == []
    assert result["clean_rows"][0]["customer_email"] == "nova@example.com"


def test_run_phase1_detects_invalid_date(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_date.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "102",
                "transaction_date": "03-12-2024",
                "customer_email": "nova@example.com",
                "amount": "125.40",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    assert result["phase1_summary"]["clean_rows"] == 0
    assert result["phase1_summary"]["anomalies"] == 1
    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "transaction_date"
    assert anomaly["error_type"] == "date_format_error"
    assert anomaly["anomaly_hint"] == "date token format mismatch"


def test_run_phase1_detects_upstream_style_invalid_slash_date(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_slash_date.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "102b",
                "transaction_date": "2024/03/12",
                "customer_email": "nova@example.com",
                "amount": "125.40",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "transaction_date"
    assert anomaly["error_type"] == "date_format_error"


def test_run_phase1_detects_invalid_email(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_email.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "103",
                "transaction_date": "2024-03-12",
                "customer_email": "not-an-email",
                "amount": "125.40",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "customer_email"
    assert anomaly["error_type"] == "email_format_error"
    assert anomaly["anomaly_hint"] == "email token malformed address"


def test_run_phase1_detects_upstream_style_double_dot_email(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_double_dot_email.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "103b",
                "transaction_date": "2024-03-12",
                "customer_email": "user..dot@mail.com",
                "amount": "125.40",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "customer_email"
    assert anomaly["error_type"] == "email_format_error"


def test_run_phase1_detects_invalid_numeric(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_numeric.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "104",
                "transaction_date": "2024-03-12",
                "customer_email": "nova@example.com",
                "amount": "12,5O",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "amount"
    assert anomaly["error_type"] == "numeric_format_error"
    assert anomaly["anomaly_hint"] == "amount contains numeric separator noise"


def test_run_phase1_detects_upstream_style_whitespace_numeric(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_whitespace_numeric.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "104b",
                "transaction_date": "2024-03-12",
                "customer_email": "nova@example.com",
                "amount": " 999.00 ",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "amount"
    assert anomaly["error_type"] == "numeric_format_error"


def test_run_phase1_detects_null_like_values(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_null.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "105",
                "transaction_date": "2024-03-12",
                "customer_email": "missing",
                "amount": "125.40",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))

    anomaly = result["anomalies"][0]
    assert anomaly["column_name"] == "customer_email"
    assert anomaly["error_type"] == "null_block_error"
    assert anomaly["anomaly_hint"] == "value is missing or sentinel null"


def test_run_phase1_writes_phase2_ready_anomaly_json(tmp_path: Path) -> None:
    input_path = tmp_path / "data" / "raw" / "bad_payload.csv"
    _write_csv(
        input_path,
        [
            {
                "id": "106",
                "transaction_date": "03-12-2024",
                "customer_email": "bad-email",
                "amount": "12,5O",
            }
        ],
    )

    result = run(_base_context(tmp_path, input_path))
    anomaly_file = Path(result["phase1_anomaly_file"])
    payload = json.loads(anomaly_file.read_text(encoding="utf-8"))

    assert isinstance(payload, list)
    assert len(payload) == 3
    assert {"id", "column_name", "value", "error_type", "anomaly_hint"}.issubset(payload[0].keys())
