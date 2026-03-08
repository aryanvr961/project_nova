"""
Module: PHASE 2 - ANOMALY CLUSTERING
Owner: Aryan
Purpose:
- Group similar anomalies using vector similarity.
Responsibilities:
- Generate embeddings.
- Store embeddings in vector database.
- Perform semantic clustering.
- Produce anomaly clusters.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, cast

from utils.embedding_engine import generate_embeddings


ANOMALY_DIR = Path("data/anomalies")
CHROMA_DIR = Path("data/vault/chromadb")
COLLECTION_NAME = "nova_anomalies"
SIMILARITY_THRESHOLD = 0.85
MIN_SAMPLE_SIZE = 3
MAX_SAMPLE_SIZE = 5


def _load_anomaly_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not ANOMALY_DIR.exists():
        return rows

    for file_path in sorted(ANOMALY_DIR.iterdir()):
        if not file_path.is_file():
            continue
        suffix = file_path.suffix.lower()
        if suffix == ".json":
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                rows.extend(item for item in payload if isinstance(item, dict))
            elif isinstance(payload, dict):
                rows.append(payload)
        elif suffix == ".jsonl":
            for line in file_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
        elif suffix == ".csv":
            try:
                import polars as pl

                records = pl.read_csv(file_path).to_dicts()
                rows.extend(record for record in records if isinstance(record, dict))
            except Exception as exc:
                print(f"[Phase 2] Could not read CSV {file_path.name}: {exc}")
        elif suffix == ".parquet":
            try:
                import polars as pl

                records = pl.read_parquet(file_path).to_dicts()
                rows.extend(record for record in records if isinstance(record, dict))
            except Exception as exc:
                print(f"[Phase 2] Could not read Parquet {file_path.name}: {exc}")
    return rows


def _row_to_text(row: dict[str, Any]) -> str:
    items = [f"{key}={row[key]}" for key in sorted(row.keys())]
    return " ".join(items)


def _row_id(row: dict[str, Any], text: str) -> str:
    if "id" in row:
        return f"row_{row['id']}"
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    return f"row_{digest}"


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a)) or 1.0
    norm_b = math.sqrt(sum(b * b for b in vec_b)) or 1.0
    return dot / (norm_a * norm_b)


def _store_in_chromadb(ids: list[str], texts: list[str], embeddings: list[list[float]], rows: list[dict[str, Any]]) -> None:
    try:
        import chromadb
        from chromadb.api.types import Embedding, Metadata
    except Exception as exc:
        print(f"[Phase 2] ChromaDB unavailable: {exc}")
        return

    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(name=COLLECTION_NAME)
    chroma_embeddings: list[Embedding] = [cast(Embedding, vector) for vector in embeddings]
    metadatas: list[Metadata] = [
        cast(Metadata, {"text": text, "row": json.dumps(row, default=str)})
        for text, row in zip(texts, rows)
    ]
    collection.upsert(ids=ids, embeddings=chroma_embeddings, metadatas=metadatas, documents=texts)


def _cluster_embeddings(
    ids: list[str],
    texts: list[str],
    rows: list[dict[str, Any]],
    embeddings: list[list[float]],
    similarity_threshold: float,
) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []

    for index, embedding in enumerate(embeddings):
        assigned_cluster = None
        for cluster in clusters:
            score = _cosine_similarity(embedding, cluster["centroid"])
            if score >= similarity_threshold:
                assigned_cluster = cluster
                break

        if assigned_cluster is None:
            clusters.append(
                {
                    "cluster_id": f"cluster_{len(clusters) + 1}",
                    "member_ids": [ids[index]],
                    "member_texts": [texts[index]],
                    "member_rows": [rows[index]],
                    "embeddings": [embedding],
                    "centroid": embedding[:],
                }
            )
            continue

        assigned_cluster["member_ids"].append(ids[index])
        assigned_cluster["member_texts"].append(texts[index])
        assigned_cluster["member_rows"].append(rows[index])
        assigned_cluster["embeddings"].append(embedding)

        count = len(assigned_cluster["embeddings"])
        dim = len(embedding)
        assigned_cluster["centroid"] = [
            sum(vec[d] for vec in assigned_cluster["embeddings"]) / count for d in range(dim)
        ]

    return clusters


def _sample_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    unique_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = json.dumps(row, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    sample_size = min(MAX_SAMPLE_SIZE, max(MIN_SAMPLE_SIZE, min(len(unique_rows), MAX_SAMPLE_SIZE)))
    return unique_rows[:sample_size]


def _pattern_key(member_texts: list[str], centroid: list[float]) -> str:
    normalized_texts = sorted(text.strip().lower() for text in member_texts)
    rounded_centroid = [round(value, 6) for value in centroid]
    payload = json.dumps(
        {"texts": normalized_texts, "centroid": rounded_centroid},
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run(context: dict) -> dict:
    """Run Phase-2 semantic clustering and return updated pipeline context."""
    print("[Phase 2] Starting anomaly clustering")
    updated_context = dict(context or {})
    anomaly_rows = updated_context.get("anomalies")
    if not isinstance(anomaly_rows, list):
        anomaly_rows = _load_anomaly_rows()

    normalized_rows = [row for row in anomaly_rows if isinstance(row, dict)]
    if not normalized_rows:
        print("[Phase 2] No anomaly rows found")
        updated_context["clusters"] = []
        updated_context["phase2_status"] = "no_anomalies"
        return updated_context

    texts = [_row_to_text(row) for row in normalized_rows]
    embeddings = generate_embeddings(texts)
    ids = [_row_id(row, text) for row, text in zip(normalized_rows, texts)]
    _store_in_chromadb(ids, texts, embeddings, normalized_rows)

    raw_clusters = _cluster_embeddings(
        ids=ids,
        texts=texts,
        rows=normalized_rows,
        embeddings=embeddings,
        similarity_threshold=SIMILARITY_THRESHOLD,
    )

    pattern_cache = updated_context.get("pattern_cache")
    if not isinstance(pattern_cache, dict):
        pattern_cache = {}

    output_clusters: list[dict[str, Any]] = []
    for cluster in raw_clusters:
        sample_rows = _sample_rows(cluster["member_rows"])
        key = _pattern_key(cluster["member_texts"], cluster["centroid"])
        cached_cluster_id = pattern_cache.get(key)
        if cached_cluster_id is None:
            pattern_cache[key] = cluster["cluster_id"]

        output_clusters.append(
            {
                "cluster_id": cluster["cluster_id"],
                "sample_rows": sample_rows,
                "size": len(cluster["member_rows"]),
                "member_ids": cluster["member_ids"],
                "cache_hit": cached_cluster_id is not None,
                "pattern_key": key,
            }
        )

    updated_context["clusters"] = output_clusters
    updated_context["pattern_cache"] = pattern_cache
    updated_context["phase2_status"] = "completed"
    updated_context["phase2_metrics"] = {
        "input_anomalies": len(normalized_rows),
        "clusters_formed": len(output_clusters),
        "semantic_compression_ratio": round(len(normalized_rows) / max(1, len(output_clusters)), 2),
    }

    print(
        f"[Phase 2] Completed: {len(normalized_rows)} anomalies -> {len(output_clusters)} clusters"
    )
    return updated_context
