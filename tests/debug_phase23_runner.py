"""
Phase 2 -> Phase 3 debug runner for integration validation.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phases.phase2_clustering import run as run_phase2
from phases.phase3_slm_remediation import run as run_phase3


DATASET_PATH = Path("data/anomalies/test_phase23_anomalies.json")


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Expected a list of anomaly rows in the JSON dataset.")
    return [row for row in payload if isinstance(row, dict)]


def _print_phase2(result: dict[str, Any]) -> None:
    clusters = result.get("clusters", [])
    print("\n=== Phase 2 Result ===")
    print(json.dumps(result.get("phase2_metrics", {}), indent=2))
    for cluster in clusters:
        sample_rows = cluster.get("sample_rows", [])[:2]
        print(
            f"- {cluster.get('cluster_id')}: size={cluster.get('size')} "
            f"cache_hit={cluster.get('cache_hit')} pattern_key={cluster.get('pattern_key', '')[:12]}..."
        )
        print(f"  sample_rows: {json.dumps(sample_rows, default=str)}")


def _print_phase3(result: dict[str, Any]) -> None:
    remediations = result.get("remediations", [])
    print("\n=== Phase 3 Result ===")
    print(json.dumps(result.get("phase3_summary", {}), indent=2))
    for remediation in remediations:
        preview = {
            "cluster_id": remediation.get("cluster_id"),
            "transformation_type": remediation.get("transformation_type"),
            "confidence_score": remediation.get("confidence_score"),
            "inferred_anomaly_type": remediation.get("inferred_anomaly_type"),
            "model_used": remediation.get("model_used"),
            "reasoning": remediation.get("reasoning"),
        }
        print(json.dumps(preview, indent=2))


def main() -> None:
    provider = os.getenv("PHASE3_PROVIDER", "mock")
    rows = _load_rows(DATASET_PATH)
    print(f"Loaded anomalies: {len(rows)} from {DATASET_PATH}")
    print(f"Phase 3 provider: {provider}")

    phase2_context = {"anomalies": rows, "pattern_cache": {}}
    phase2_result = run_phase2(phase2_context)
    _print_phase2(phase2_result)

    phase3_result = run_phase3(phase2_result)
    _print_phase3(phase3_result)


if __name__ == "__main__":
    main()
