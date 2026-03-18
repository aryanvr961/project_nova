"""
Phase 1 debug runner for deterministic ingestion validation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phases.phase1_ingestion import run


DEFAULT_INPUT_PATH = Path("data/raw/sample_data.csv")


def _build_context(input_path: Path) -> dict:
    return {
        "input_path": str(input_path),
        "source_name": input_path.stem,
        "clean_dir": "data/clean",
        "anomalies_dir": "data/anomalies",
        "vault_dir": "data/vault",
    }


def _print_result(result: dict) -> None:
    print("\n=== Phase 1 Result ===")
    print(json.dumps(result.get("phase1_summary", {}), indent=2))
    print(f"Phase 1 status: {result.get('phase1_status')}")
    print(f"Clean output: {result.get('phase1_clean_file')}")
    print(f"Anomaly output: {result.get('phase1_anomaly_file')}")

    clean_rows = result.get("clean_rows", [])
    anomalies = result.get("anomalies", [])

    print(f"Clean row count: {len(clean_rows)}")
    print(f"Anomaly count: {len(anomalies)}")

    if clean_rows:
        print("Clean preview:")
        print(json.dumps(clean_rows[:2], indent=2, default=str))

    if anomalies:
        print("Anomaly preview:")
        print(json.dumps(anomalies[:5], indent=2, default=str))


def main() -> None:
    input_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
    if not input_arg.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {input_arg}. Pass a CSV/JSON/JSONL path as an argument."
        )

    print(f"Running Phase 1 with input: {input_arg}")
    result = run(_build_context(input_arg))
    _print_result(result)


if __name__ == "__main__":
    main()
