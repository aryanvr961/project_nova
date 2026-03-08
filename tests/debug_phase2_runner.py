"""
Phase-2 debug runner for end-to-end clustering validation.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

# Ensure project root is importable when running this file directly.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phases.phase2_clustering import run


DATASET_PATH = Path("data/anomalies/test_anomalies.csv")
CHROMA_DB_PATH = Path("data/vault/chromadb/chroma.sqlite3")


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = [dict(row) for row in reader]
    for row in rows:
        if "id" in row:
            try:
                row["id"] = int(row["id"])
            except (TypeError, ValueError):
                pass
    return rows


def _print_result(result: dict[str, Any], run_label: str) -> None:
    clusters = result.get("clusters", [])
    metrics = result.get("phase2_metrics", {})
    print(f"\n=== {run_label} ===")
    print(f"Clusters created: {len(clusters)}")
    print(f"Metrics: {json.dumps(metrics, indent=2)}")
    for cluster in clusters:
        sample_preview = cluster.get("sample_rows", [])[:3]
        print(
            f"- {cluster.get('cluster_id')}: size={cluster.get('size')} "
            f"cache_hit={cluster.get('cache_hit')} pattern_key={cluster.get('pattern_key')[:12]}..."
        )
        print(f"  sample_rows(<=3 preview): {json.dumps(sample_preview, default=str)}")


def _check_chroma_persistence() -> None:
    print("\n=== ChromaDB Check ===")
    print(f"Expected DB file: {CHROMA_DB_PATH}")
    if CHROMA_DB_PATH.exists():
        print(f"Exists: True, Size(bytes): {CHROMA_DB_PATH.stat().st_size}")
    else:
        print("Exists: False")


def main() -> None:
    rows = _load_rows(DATASET_PATH)
    print(f"Loaded anomalies: {len(rows)} from {DATASET_PATH}")

    context = {"anomalies": rows, "pattern_cache": {}}
    first_result = run(context)
    _print_result(first_result, "Run #1")
    _check_chroma_persistence()

    second_context = {
        "anomalies": rows,
        "pattern_cache": dict(first_result.get("pattern_cache", {})),
    }
    second_result = run(second_context)
    _print_result(second_result, "Run #2 (cache validation)")
    cache_hits = sum(1 for cluster in second_result.get("clusters", []) if cluster.get("cache_hit"))
    print(f"Cache-hit clusters in run #2: {cache_hits}/{len(second_result.get('clusters', []))}")


if __name__ == "__main__":
    main()
