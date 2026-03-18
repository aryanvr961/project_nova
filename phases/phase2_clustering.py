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

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any, cast

from utils.embedding_engine import generate_embeddings


ANOMALY_DIR = Path("data/anomalies")
VAULT_DIR = Path("data/vault")
CHROMA_DIR = VAULT_DIR / "chromadb"
COLLECTION_NAME = "nova_anomalies"
CLUSTER_MEMORY_COLLECTION = "nova_cluster_memory"
CLUSTER_REGISTRY_FILE = VAULT_DIR / "cluster_registry.json"
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
    anomaly_uid = row.get("anomaly_uid")
    if anomaly_uid not in (None, ""):
        return f"anomaly_{anomaly_uid}"
    if "id" in row:
        column_name = row.get("column_name")
        error_type = row.get("error_type")
        if column_name not in (None, "") or error_type not in (None, ""):
            suffix = f"{column_name}:{error_type}"
            return f"row_{row['id']}:{suffix}"
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


def _load_cluster_registry() -> dict[str, Any]:
    if not CLUSTER_REGISTRY_FILE.exists():
        return {"version": 1, "next_id": 1, "patterns": {}}
    try:
        payload = json.loads(CLUSTER_REGISTRY_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload.setdefault("version", 1)
            payload.setdefault("next_id", 1)
            payload.setdefault("patterns", {})
            return payload
    except Exception:
        pass
    return {"version": 1, "next_id": 1, "patterns": {}}


def _save_cluster_registry(registry: dict[str, Any]) -> None:
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    CLUSTER_REGISTRY_FILE.write_text(
        json.dumps(registry, ensure_ascii=True, indent=2, default=str),
        encoding="utf-8",
    )


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _assign_cluster_uid(
    *,
    pattern_key: str,
    runtime_cluster_id: str,
    registry: dict[str, Any],
) -> str:
    patterns = registry.setdefault("patterns", {})
    entry = patterns.get(pattern_key)
    if isinstance(entry, dict) and isinstance(entry.get("cluster_uid"), str):
        entry["last_seen_at"] = _now_utc()
        entry["seen_count"] = int(entry.get("seen_count", 0)) + 1
        entry["latest_runtime_cluster_id"] = runtime_cluster_id
        return entry["cluster_uid"]

    next_id = int(registry.get("next_id", 1))
    cluster_uid = f"nova_cluster_{next_id:05d}"
    registry["next_id"] = next_id + 1
    patterns[pattern_key] = {
        "cluster_uid": cluster_uid,
        "first_seen_at": _now_utc(),
        "last_seen_at": _now_utc(),
        "seen_count": 1,
        "latest_runtime_cluster_id": runtime_cluster_id,
    }
    return cluster_uid


def _infer_target_column(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        if not isinstance(row, dict) or not row:
            continue
        for preferred in ("column_name", "field", "column"):
            if preferred in row and row.get(preferred):
                return str(row.get(preferred))
        if "value" in row and len(row) > 1:
            for key in row.keys():
                if key != "value":
                    return str(key)
        return str(next(iter(row.keys())))
    return ""


def _build_cluster_document(cluster: dict[str, Any], sample_rows: list[dict[str, Any]]) -> str:
    target_column = _infer_target_column(sample_rows)
    sample_values: list[str] = []
    for row in sample_rows[:5]:
        if isinstance(row, dict):
            if "value" in row:
                sample_values.append(str(row.get("value")))
            elif target_column and target_column in row:
                sample_values.append(str(row.get(target_column)))
    payload = {
        "cluster_uid": cluster["cluster_uid"],
        "runtime_cluster_id": cluster["cluster_id"],
        "pattern_key": cluster["pattern_key"],
        "size": cluster["size"],
        "target_column": target_column,
        "sample_values": sample_values,
    }
    return json.dumps(payload, ensure_ascii=True, default=str)


def _store_cluster_memory_in_chromadb(cluster_documents: list[dict[str, Any]]) -> None:
    if not cluster_documents:
        return
    try:
        import chromadb
        from chromadb.api.types import Embedding, Metadata
    except Exception as exc:
        print(f"[Phase 2] Cluster memory store unavailable: {exc}")
        return

    texts = [item["document"] for item in cluster_documents]
    embeddings = generate_embeddings(texts)
    ids = [item["cluster_uid"] for item in cluster_documents]

    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_DIR))
    collection = client.get_or_create_collection(name=CLUSTER_MEMORY_COLLECTION)
    chroma_embeddings: list[Embedding] = [cast(Embedding, vector) for vector in embeddings]
    metadatas: list[Metadata] = [
        cast(
            Metadata,
            {
                "cluster_uid": item["cluster_uid"],
                "runtime_cluster_id": item["cluster_id"],
                "pattern_key": item["pattern_key"],
                "size": item["size"],
                "target_column": item["target_column"],
                "sample_rows_json": json.dumps(item["sample_rows"], ensure_ascii=True, default=str),
                "member_ids_json": json.dumps(item["member_ids"], ensure_ascii=True, default=str),
            },
        )
        for item in cluster_documents
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

    cluster_registry = _load_cluster_registry()
    output_clusters: list[dict[str, Any]] = []
    cluster_documents: list[dict[str, Any]] = []
    for cluster in raw_clusters:
        sample_rows = _sample_rows(cluster["member_rows"])
        key = _pattern_key(cluster["member_texts"], cluster["centroid"])
        cluster_uid = _assign_cluster_uid(
            pattern_key=key,
            runtime_cluster_id=cluster["cluster_id"],
            registry=cluster_registry,
        )
        cached_cluster_id = pattern_cache.get(key)
        if cached_cluster_id is None:
            pattern_cache[key] = cluster_uid

        target_column = _infer_target_column(sample_rows)
        cluster_documents.append(
            {
                "cluster_uid": cluster_uid,
                "cluster_id": cluster["cluster_id"],
                "pattern_key": key,
                "size": len(cluster["member_rows"]),
                "sample_rows": sample_rows,
                "member_ids": cluster["member_ids"],
                "target_column": target_column,
                "document": _build_cluster_document(
                    {
                        "cluster_uid": cluster_uid,
                        "cluster_id": cluster["cluster_id"],
                        "pattern_key": key,
                        "size": len(cluster["member_rows"]),
                    },
                    sample_rows,
                ),
            }
        )

        output_clusters.append(
            {
                "cluster_id": cluster["cluster_id"],
                "cluster_uid": cluster_uid,
                "sample_rows": sample_rows,
                "size": len(cluster["member_rows"]),
                "member_ids": cluster["member_ids"],
                "cache_hit": cached_cluster_id is not None,
                "pattern_key": key,
                "target_column": target_column,
            }
        )

    _save_cluster_registry(cluster_registry)
    _store_cluster_memory_in_chromadb(cluster_documents)

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
