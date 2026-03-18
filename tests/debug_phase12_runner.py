"""
Phase 1 -> Phase 2 debug runner for integration validation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phases.phase1_ingestion import run as run_phase1
from phases.phase2_clustering import run as run_phase2


def _sample_rows() -> list[dict[str, str]]:
    return [
        {"id": "1", "transaction_date": "03-12-2024", "customer_email": "alice@example.com", "amount": "100.50"},
        {"id": "2", "transaction_date": "2024/03/12", "customer_email": "bob@example.com", "amount": "99.10"},
        {"id": "3", "transaction_date": "2024-03-12", "customer_email": "bad@", "amount": "110.00"},
        {"id": "4", "transaction_date": "2024-03-12", "customer_email": "carol@example.com", "amount": "1,20x"},
        {"id": "5", "transaction_date": "2024-03-12", "customer_email": "", "amount": "42.00"},
    ]


def main() -> None:
    rows = _sample_rows()
    print(f"Loaded raw rows: {len(rows)}")

    phase1_result = run_phase1({"raw_rows": rows, "required_columns": ["transaction_date", "customer_email", "amount"]})
    print("\n=== Phase 1 Result ===")
    print(json.dumps(phase1_result.get("phase1_summary", {}), indent=2))
    print(f"Anomaly records emitted: {len(phase1_result.get('anomalies', []))}")

    phase2_result = run_phase2(phase1_result)
    print("\n=== Phase 2 Result ===")
    print(json.dumps(phase2_result.get("phase2_metrics", {}), indent=2))
    print(f"Clusters formed: {len(phase2_result.get('clusters', []))}")


if __name__ == "__main__":
    main()
