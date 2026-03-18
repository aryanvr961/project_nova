"""
Module: PHASE 3 - AI REMEDIATION ENGINE
Owner: Aryan
Purpose:
- Generate structured fix suggestions from anomaly clusters.
Responsibilities:
- Normalize Phase 2 clusters into remediation inputs.
- Attach prompt and rule context for retrieval-ready prompting.
- Route requests through a configurable provider backend.
- Return audited remediation payloads for downstream execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from utils.embedding_engine import generate_embeddings
from utils.phase3_slm_provider import call_phase3_provider, get_phase3_provider

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path("prompts")
VAULT_DIR = Path("data/vault")
CHROMA_DIR = VAULT_DIR / "chromadb"
REMEDIATION_MEMORY_FILE = VAULT_DIR / "remediation_memory.jsonl"

REMEDIATION_PROMPT_FILE = PROMPTS_DIR / "remediation_v1.txt"
SYSTEM_RULES_FILE = PROMPTS_DIR / "system_rules.txt"

CLUSTER_MEMORY_COLLECTION = "nova_cluster_memory"
REMEDIATION_MEMORY_COLLECTION = "nova_remediation_memory"

CONFIDENCE_THRESHOLD = 0.75
RETRIEVAL_TOP_K = 5
VALID_TRANSFORMATION_TYPES = {"lambda", "rule", "quarantine"}
FAST_PATH_ANOMALY_TYPES = {"date_format", "type_cast", "null_fill"}

DATE_PATTERN = re.compile(r"\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|[A-Za-z]+ \d{1,2} \d{4}")
NUMERIC_PATTERN = re.compile(r"^-?\d+([.,]\d+)?$")

SYSTEM_PROMPT = """You are a deterministic data transformation engine inside an enterprise ETL pipeline.

Your job is to analyze one anomaly cluster and output a single JSON object only.
No markdown, no code fences, no explanation outside JSON.

Required output schema:
{
  "transformation_type": "lambda",
  "code": "<single-line Python lambda>",
  "confidence_score": <float 0.0-1.0>,
  "reasoning": "<one sentence>",
  "fallback_value": "<safe fallback>"
}

Rules:
1. Lambda must be a single expression only.
2. Use Python built-ins only, no imports.
3. Handle None and NaN safely.
4. If confidence is below 0.75, choose a conservative result.
5. Do not invent fields or values outside the given cluster context."""


@dataclass
class ClusterInput:
    cluster_id: str
    cluster_uid: str
    sample_rows: list[Any]
    size: int
    member_ids: list[str]
    cache_hit: bool
    pattern_key: str
    inferred_anomaly_type: str = "unknown"
    sample_values: list[str] = field(default_factory=list)
    rule_context: dict[str, Any] = field(default_factory=dict)
    target_column: str = ""
    cluster_profile: dict[str, Any] = field(default_factory=dict)


@dataclass
class RemediationResult:
    cluster_id: str
    cluster_uid: str
    transformation_type: str
    code: str
    confidence_score: float
    reasoning: str
    fallback_value: str
    inferred_anomaly_type: str
    model_used: str
    member_ids: list[str]
    size: int
    cache_hit: bool
    pattern_key: str
    guardrail_action: str
    risk_level: str
    requires_human_review: bool
    validation_checks: list[str]
    raw_response: str | None = None


def _read_text_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _extract_sample_values(sample_rows: list[Any], limit: int = 5) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()

    for row in sample_rows:
        candidates: list[Any]
        if isinstance(row, dict):
            if "value" in row:
                candidates = [row.get("value")]
            else:
                preferred_keys = [key for key in ("column_name", "date", "email", "phone", "amount", "value") if key in row]
                if preferred_keys:
                    candidates = [row.get(preferred_keys[0])]
                else:
                    candidates = list(row.values())
        else:
            candidates = [row]

        for candidate in candidates:
            text = str(candidate)
            if text in seen:
                continue
            seen.add(text)
            values.append(text)
            if len(values) >= limit:
                return values

    return values


def _infer_anomaly_type(sample_values: list[str]) -> str:
    non_null = [
        value
        for value in sample_values
        if value.lower() not in {"none", "nan", "null", ""}
    ]
    if not non_null:
        return "null_fill"

    date_hits = sum(1 for value in non_null if DATE_PATTERN.search(value))
    numeric_hits = sum(
        1 for value in non_null if NUMERIC_PATTERN.match(value.replace(",", "").replace(" ", "").strip())
    )

    if date_hits >= max(1, math.ceil(len(non_null) * 0.6)):
        return "date_format"
    if numeric_hits >= max(1, math.ceil(len(non_null) * 0.6)):
        return "type_cast"
    if len(non_null) < len(sample_values):
        return "null_fill"
    return "string_corrupt"


def _infer_target_column(sample_rows: list[Any]) -> str:
    for row in sample_rows:
        if not isinstance(row, dict) or not row:
            continue
        for candidate in ("column_name", "field", "column"):
            value = row.get(candidate)
            if value:
                return str(value)
        if "value" in row and len(row) > 1:
            for key in row.keys():
                if key != "value":
                    return str(key)
        return str(next(iter(row.keys())))
    return ""


def _build_cluster_profile(cluster: ClusterInput) -> dict[str, Any]:
    non_null_values = [value for value in cluster.sample_values if value.lower() not in {"none", "null", "nan", ""}]
    null_ratio = 0.0
    if cluster.sample_values:
        null_ratio = round(1.0 - (len(non_null_values) / len(cluster.sample_values)), 3)

    unique_ratio = 0.0
    if cluster.sample_values:
        unique_ratio = round(len(set(cluster.sample_values)) / len(cluster.sample_values), 3)

    hints: dict[str, int] = {}
    for row in cluster.sample_rows:
        if not isinstance(row, dict):
            continue
        for key in ("error_type", "anomaly_hint"):
            value = row.get(key)
            if value:
                label = str(value)
                hints[label] = hints.get(label, 0) + 1

    severity = "low"
    if cluster.size >= 10 or cluster.inferred_anomaly_type in {"string_corrupt", "date_format"}:
        severity = "medium"
    if cluster.size >= 25:
        severity = "high"

    return {
        "sample_count": len(cluster.sample_rows),
        "unique_sample_ratio": unique_ratio,
        "null_ratio": null_ratio,
        "hint_distribution": hints,
        "estimated_severity": severity,
    }


def _cluster_hint_count(cluster: ClusterInput, label: str) -> int:
    return int(cluster.cluster_profile.get("hint_distribution", {}).get(label, 0) or 0)


def _is_duplicate_cluster(cluster: ClusterInput) -> bool:
    return _cluster_hint_count(cluster, "duplicate_value_error") > 0


def _is_fast_path_cluster(cluster: ClusterInput) -> bool:
    if cluster.inferred_anomaly_type in FAST_PATH_ANOMALY_TYPES:
        return True
    if _is_duplicate_cluster(cluster):
        return True
    return False


def _load_static_rule_documents() -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for source_path in (SYSTEM_RULES_FILE, REMEDIATION_PROMPT_FILE):
        text = _read_text_file(source_path)
        if not text:
            continue
        for index, chunk in enumerate(line.strip() for line in text.splitlines() if line.strip()):
            documents.append(
                {
                    "id": f"{source_path.stem}_{index + 1}",
                    "source": source_path.name,
                    "text": chunk,
                    "source_weight": 0.3,
                }
            )
    return documents


def _load_cluster_memory_documents(cluster: ClusterInput) -> list[dict[str, Any]]:
    try:
        import chromadb
    except Exception:
        return []

    query_text = " ".join(
        [
            cluster.pattern_key,
            cluster.target_column,
            cluster.inferred_anomaly_type,
            *cluster.sample_values,
        ]
    ).strip()
    if not query_text:
        return []

    try:
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection(name=CLUSTER_MEMORY_COLLECTION)
        result = collection.query(
            query_texts=[query_text],
            n_results=RETRIEVAL_TOP_K,
            include=["documents", "metadatas", "distances"],
        )
    except Exception:
        return []

    documents = result.get("documents", [[]])
    metadatas = result.get("metadatas", [[]])
    distances = result.get("distances", [[]])
    output: list[dict[str, Any]] = []
    for index, text in enumerate(documents[0] if documents else []):
        metadata = (metadatas[0][index] if metadatas and metadatas[0] else {}) or {}
        distance = float((distances[0][index] if distances and distances[0] else 0.0) or 0.0)
        source_weight = max(0.0, 1.0 - min(1.0, distance))
        output.append(
            {
                "id": str(metadata.get("cluster_uid") or f"cluster_memory_{index + 1}"),
                "source": "chroma_cluster_memory",
                "text": str(text),
                "source_weight": round(source_weight, 4),
            }
        )
    return output


def _load_historical_remediation_documents(cluster: ClusterInput) -> list[dict[str, Any]]:
    if not REMEDIATION_MEMORY_FILE.exists():
        return []

    rows: list[dict[str, Any]] = []
    for line in REMEDIATION_MEMORY_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
        except Exception:
            continue

    scored: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        score = 0.0
        if row.get("pattern_key") == cluster.pattern_key:
            score += 0.7
        if row.get("inferred_anomaly_type") == cluster.inferred_anomaly_type:
            score += 0.2
        if row.get("target_column") == cluster.target_column:
            score += 0.1
        if score <= 0.0:
            continue

        summary = {
            "previous_action": row.get("transformation_type"),
            "confidence": row.get("confidence_score"),
            "code": row.get("code"),
            "reasoning": row.get("reasoning"),
        }
        scored.append(
            (
                score,
                {
                    "id": str(row.get("memory_id") or f"history_{len(scored) + 1}"),
                    "source": "historical_remediation_memory",
                    "text": json.dumps(summary, ensure_ascii=True, default=str),
                    "source_weight": round(score, 4),
                },
            )
        )

    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[:RETRIEVAL_TOP_K]]


def _load_retrieval_documents(cluster: ClusterInput) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    documents.extend(_load_static_rule_documents())
    if not _is_fast_path_cluster(cluster):
        documents.extend(_load_cluster_memory_documents(cluster))
        documents.extend(_load_historical_remediation_documents(cluster))
    return documents


def _tokenize(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9_]+", text.lower()) if token}


def _keyword_overlap_score(query: str, document: str) -> float:
    query_tokens = _tokenize(query)
    document_tokens = _tokenize(document)
    if not query_tokens or not document_tokens:
        return 0.0
    return len(query_tokens & document_tokens) / len(query_tokens | document_tokens)


def _embedding_similarity(query: str, document: str) -> float:
    vectors = generate_embeddings([query, document])
    if len(vectors) != 2:
        return 0.0
    vec_a, vec_b = vectors
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a)) or 1.0
    norm_b = math.sqrt(sum(b * b for b in vec_b)) or 1.0
    return dot / (norm_a * norm_b)


def _retrieve_rule_context(query: str, documents: list[dict[str, Any]], top_k: int = RETRIEVAL_TOP_K) -> list[dict[str, Any]]:
    if not query or not documents:
        return []

    scored_documents: list[dict[str, Any]] = []
    for document in documents:
        text = str(document.get("text", ""))
        keyword_score = _keyword_overlap_score(query, text)
        embedding_score = _embedding_similarity(query, text)
        source_weight = float(document.get("source_weight", 0.0) or 0.0)
        blended_score = round((0.2 * source_weight) + (0.3 * keyword_score) + (0.5 * embedding_score), 4)
        scored_documents.append(
            {
                "id": str(document.get("id", "")),
                "source": str(document.get("source", "unknown")),
                "text": text,
                "score": blended_score,
            }
        )

    scored_documents.sort(key=lambda item: item["score"], reverse=True)
    return scored_documents[:top_k]


def _build_rule_context(cluster: ClusterInput) -> dict[str, Any]:
    documents = _load_retrieval_documents(cluster)
    if _is_fast_path_cluster(cluster):
        retrieved = [
            {
                "id": str(document.get("id", "")),
                "source": str(document.get("source", "unknown")),
                "text": str(document.get("text", "")),
                "score": round(float(document.get("source_weight", 0.0) or 0.0), 4),
            }
            for document in documents[:2]
        ]
        return {
            "retrieved_rules": retrieved,
            "cluster_pattern_key": cluster.pattern_key,
            "cache_hit": cluster.cache_hit,
            "target_column": cluster.target_column,
            "retrieval_sources": sorted({item["source"] for item in retrieved}),
        }

    query_text = " ".join(
        [
            cluster.cluster_uid,
            cluster.cluster_id,
            cluster.pattern_key,
            cluster.inferred_anomaly_type,
            cluster.target_column,
            *cluster.sample_values,
        ]
    ).strip()
    retrieved = _retrieve_rule_context(query_text, documents, top_k=RETRIEVAL_TOP_K)
    return {
        "retrieved_rules": retrieved,
        "cluster_pattern_key": cluster.pattern_key,
        "cache_hit": cluster.cache_hit,
        "target_column": cluster.target_column,
        "retrieval_sources": sorted({item["source"] for item in retrieved}),
    }


def _build_user_prompt(cluster: ClusterInput) -> str:
    payload = {
        "cluster_id": cluster.cluster_id,
        "cluster_uid": cluster.cluster_uid,
        "sample_values": cluster.sample_values,
        "anomaly_type": cluster.inferred_anomaly_type,
        "size": cluster.size,
        "pattern_key": cluster.pattern_key,
        "target_column": cluster.target_column,
        "cluster_profile": cluster.cluster_profile,
        "rule_context": cluster.rule_context,
        "guardrails": {
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "quarantine_if_ambiguous": True,
        },
    }
    return json.dumps(payload, ensure_ascii=True, default=str)


def _mock_response(cluster: ClusterInput) -> tuple[str, str]:
    top_rule = ""
    retrieved_rules = cluster.rule_context.get("retrieved_rules", [])
    if retrieved_rules:
        top_rule = str(retrieved_rules[0].get("text", ""))

    if cluster.inferred_anomaly_type == "date_format":
        response = {
            "transformation_type": "rule",
            "code": "lambda x: x",
            "confidence_score": 0.82,
            "reasoning": f"Detected mixed date-like values and selected a conservative rule. {top_rule}".strip(),
            "fallback_value": "null",
        }
    elif cluster.inferred_anomaly_type == "null_fill":
        response = {
            "transformation_type": "rule",
            "code": "lambda x: x if x is not None else None",
            "confidence_score": 0.78,
            "reasoning": f"Detected mostly null-like values and chose a null-preserving rule. {top_rule}".strip(),
            "fallback_value": "null",
        }
    elif cluster.inferred_anomaly_type == "type_cast":
        response = {
            "transformation_type": "lambda",
            "code": "lambda x: None if x is None else str(x).replace(',', '')",
            "confidence_score": 0.8,
            "reasoning": f"Detected numeric-looking values with formatting noise and proposed safe normalization. {top_rule}".strip(),
            "fallback_value": "null",
        }
    else:
        response = {
            "transformation_type": "quarantine",
            "code": "lambda x: x",
            "confidence_score": 0.4,
            "reasoning": f"Pattern is ambiguous, so the cluster should be quarantined. {top_rule}".strip(),
            "fallback_value": "null",
        }
    return json.dumps(response), "mock/static"


def _call_provider(cluster: ClusterInput) -> tuple[str, str]:
    prompt = _build_user_prompt(cluster)
    return call_phase3_provider(
        prompt=prompt,
        system_prompt=SYSTEM_PROMPT,
        cluster=cluster,
        mock_response_builder=_mock_response,
    )


def _fast_path_payload(cluster: ClusterInput) -> tuple[dict[str, Any], str] | None:
    sample_value = cluster.sample_values[0] if cluster.sample_values else "null"
    fallback_value = "null"

    if _is_duplicate_cluster(cluster):
        payload = {
            "transformation_type": "rule",
            "code": "lambda x: x",
            "confidence_score": 0.93,
            "reasoning": "Detected duplicate-value cluster and selected a no-mutation deterministic hold rule.",
            "fallback_value": str(sample_value),
        }
        return payload, "deterministic/duplicate_hold"

    if cluster.inferred_anomaly_type == "date_format":
        payload = {
            "transformation_type": "rule",
            "code": "lambda x: x",
            "confidence_score": 0.91,
            "reasoning": "Detected date-format cluster and selected a deterministic staging rule for guarded downstream handling.",
            "fallback_value": str(sample_value),
        }
        return payload, "deterministic/date_format"

    if cluster.inferred_anomaly_type == "type_cast":
        payload = {
            "transformation_type": "lambda",
            "code": "lambda x: None if x is None else str(x).replace(',', '').strip()",
            "confidence_score": 0.92,
            "reasoning": "Detected numeric formatting noise and selected deterministic string normalization.",
            "fallback_value": fallback_value,
        }
        return payload, "deterministic/type_cast"

    if cluster.inferred_anomaly_type == "null_fill":
        payload = {
            "transformation_type": "rule",
            "code": "lambda x: x if x is not None else None",
            "confidence_score": 0.9,
            "reasoning": "Detected null-heavy cluster and selected a null-preserving deterministic rule.",
            "fallback_value": fallback_value,
        }
        return payload, "deterministic/null_fill"

    return None


def _load_cached_remediation(cluster: ClusterInput) -> RemediationResult | None:
    if not REMEDIATION_MEMORY_FILE.exists():
        return None

    best_row: dict[str, Any] | None = None
    best_score = -1.0
    for line in REMEDIATION_MEMORY_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue

        score = 0.0
        if row.get("pattern_key") == cluster.pattern_key:
            score += 1.0
        if row.get("target_column") == cluster.target_column:
            score += 0.2
        if row.get("inferred_anomaly_type") == cluster.inferred_anomaly_type:
            score += 0.2
        if score > best_score:
            best_score = score
            best_row = row

    if best_row is None or best_score < 1.0:
        return None

    confidence_score = float(best_row.get("confidence_score", 0.0) or 0.0)
    transformation_type = str(best_row.get("transformation_type", "quarantine"))
    code = str(best_row.get("code", "lambda x: x"))
    if confidence_score < CONFIDENCE_THRESHOLD or transformation_type not in VALID_TRANSFORMATION_TYPES:
        return None

    guardrails = _build_guardrail_plan(
        transformation_type=transformation_type,
        confidence_score=confidence_score,
        inferred_anomaly_type=cluster.inferred_anomaly_type,
    )
    return RemediationResult(
        cluster_id=cluster.cluster_id,
        cluster_uid=cluster.cluster_uid,
        transformation_type=transformation_type,
        code=code,
        confidence_score=confidence_score,
        reasoning=str(best_row.get("reasoning", "Loaded remediation from pattern cache.")),
        fallback_value=str(best_row.get("fallback_value", "null")),
        inferred_anomaly_type=cluster.inferred_anomaly_type,
        model_used="cache/remediation_memory",
        member_ids=cluster.member_ids,
        size=cluster.size,
        cache_hit=True,
        pattern_key=cluster.pattern_key,
        guardrail_action=guardrails["guardrail_action"],
        risk_level=guardrails["risk_level"],
        requires_human_review=guardrails["requires_human_review"],
        validation_checks=guardrails["validation_checks"],
        raw_response=json.dumps(best_row, ensure_ascii=True, default=str),
    )


def _build_guardrail_plan(*, transformation_type: str, confidence_score: float, inferred_anomaly_type: str) -> dict[str, Any]:
    if transformation_type == "quarantine":
        return {
            "guardrail_action": "quarantine",
            "risk_level": "high",
            "requires_human_review": True,
            "validation_checks": ["schema_check", "null_safety_check", "manual_approval"],
        }
    if confidence_score < 0.85:
        return {
            "guardrail_action": "staging_only",
            "risk_level": "medium",
            "requires_human_review": inferred_anomaly_type == "string_corrupt",
            "validation_checks": ["schema_check", "roundtrip_consistency_check", "null_safety_check"],
        }
    return {
        "guardrail_action": "auto_apply_with_audit",
        "risk_level": "low",
        "requires_human_review": False,
        "validation_checks": ["schema_check", "determinism_check"],
    }


def _quarantine(cluster: ClusterInput, model_used: str, raw: str, reason: str) -> RemediationResult:
    guardrails = _build_guardrail_plan(
        transformation_type="quarantine",
        confidence_score=0.0,
        inferred_anomaly_type=cluster.inferred_anomaly_type,
    )
    return RemediationResult(
        cluster_id=cluster.cluster_id,
        cluster_uid=cluster.cluster_uid,
        transformation_type="quarantine",
        code="lambda x: x",
        confidence_score=0.0,
        reasoning=reason,
        fallback_value="null",
        inferred_anomaly_type=cluster.inferred_anomaly_type,
        model_used=model_used,
        member_ids=cluster.member_ids,
        size=cluster.size,
        cache_hit=cluster.cache_hit,
        pattern_key=cluster.pattern_key,
        guardrail_action=guardrails["guardrail_action"],
        risk_level=guardrails["risk_level"],
        requires_human_review=guardrails["requires_human_review"],
        validation_checks=guardrails["validation_checks"],
        raw_response=raw,
    )


def _parse_response(raw: str, cluster: ClusterInput, model_used: str) -> RemediationResult:
    try:
        clean = raw.strip()
        if clean.startswith("```"):
            clean = clean.strip("`").removeprefix("json").strip()
        try:
            data = json.loads(clean)
        except json.JSONDecodeError:
            data = json.loads(_extract_json_object(clean))
        confidence_score = float(data.get("confidence_score", 0.0))
        transformation_type = str(data.get("transformation_type", "rule"))
        code = str(data.get("code", "lambda x: x"))
        if transformation_type not in VALID_TRANSFORMATION_TYPES:
            return _quarantine(
                cluster,
                model_used,
                raw,
                f"Invalid transformation_type: {transformation_type}",
            )
        if transformation_type in {"lambda", "rule"} and "lambda" not in code:
            return _quarantine(
                cluster,
                model_used,
                raw,
                "Invalid code format: lambda expression required.",
            )
        if confidence_score < CONFIDENCE_THRESHOLD:
            return _quarantine(
                cluster,
                model_used,
                raw,
                f"Low confidence remediation ({confidence_score:.2f}).",
            )

        guardrails = _build_guardrail_plan(
            transformation_type=transformation_type,
            confidence_score=confidence_score,
            inferred_anomaly_type=cluster.inferred_anomaly_type,
        )
        return RemediationResult(
            cluster_id=cluster.cluster_id,
            cluster_uid=cluster.cluster_uid,
            transformation_type=transformation_type,
            code=code,
            confidence_score=confidence_score,
            reasoning=str(data.get("reasoning", "")),
            fallback_value=str(data.get("fallback_value", "null")),
            inferred_anomaly_type=cluster.inferred_anomaly_type,
            model_used=model_used,
            member_ids=cluster.member_ids,
            size=cluster.size,
            cache_hit=cluster.cache_hit,
            pattern_key=cluster.pattern_key,
            guardrail_action=guardrails["guardrail_action"],
            risk_level=guardrails["risk_level"],
            requires_human_review=guardrails["requires_human_review"],
            validation_checks=guardrails["validation_checks"],
            raw_response=raw,
        )
    except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        return _quarantine(cluster, model_used, raw, f"Parse error: {exc}")


def _extract_json_object(text: str) -> str:
    start = text.find("{")
    if start < 0:
        raise json.JSONDecodeError("No JSON object start found", text, 0)
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start:index + 1]
    raise json.JSONDecodeError("Unterminated JSON object", text, start)


def _normalize_cluster(raw_cluster: dict[str, Any]) -> ClusterInput:
    sample_rows = raw_cluster.get("sample_rows", [])
    normalized_sample_rows = sample_rows if isinstance(sample_rows, list) else []
    sample_values = _extract_sample_values(normalized_sample_rows)
    target_column = str(raw_cluster.get("target_column") or "") or _infer_target_column(normalized_sample_rows)
    cluster_uid = str(raw_cluster.get("cluster_uid") or raw_cluster.get("cluster_id") or raw_cluster.get("pattern_key") or "cluster_unknown")

    cluster = ClusterInput(
        cluster_id=str(raw_cluster["cluster_id"]),
        cluster_uid=cluster_uid,
        sample_rows=normalized_sample_rows,
        size=int(raw_cluster.get("size", 0)),
        member_ids=[str(member_id) for member_id in raw_cluster.get("member_ids", [])],
        cache_hit=bool(raw_cluster.get("cache_hit", False)),
        pattern_key=str(raw_cluster.get("pattern_key", "")),
        sample_values=sample_values,
        target_column=target_column,
    )
    cluster.inferred_anomaly_type = _infer_anomaly_type(cluster.sample_values)
    cluster.cluster_profile = _build_cluster_profile(cluster)
    cluster.rule_context = _build_rule_context(cluster)
    return cluster


def _remediate_cluster(cluster: ClusterInput) -> RemediationResult:
    cached = _load_cached_remediation(cluster)
    if cached is not None:
        return cached

    fast_path = _fast_path_payload(cluster)
    if fast_path is not None:
        payload, model_used = fast_path
        return _parse_response(json.dumps(payload, ensure_ascii=True), cluster, model_used)

    try:
        raw, model_used = _call_provider(cluster)
    except Exception as exc:
        logger.error("[Phase 3] Provider failed for %s: %s", cluster.cluster_id, exc)
        return _quarantine(cluster, "none", "", f"Provider failure: {exc}")
    return _parse_response(raw, cluster, model_used)


def _record_remediation_memory(cluster: ClusterInput, result: RemediationResult) -> None:
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    memory_id = f"rem_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}_{cluster.cluster_uid}"
    payload = {
        "memory_id": memory_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "cluster_id": cluster.cluster_id,
        "cluster_uid": cluster.cluster_uid,
        "pattern_key": cluster.pattern_key,
        "target_column": cluster.target_column,
        "inferred_anomaly_type": cluster.inferred_anomaly_type,
        "transformation_type": result.transformation_type,
        "code": result.code,
        "confidence_score": result.confidence_score,
        "reasoning": result.reasoning,
        "guardrail_action": result.guardrail_action,
        "risk_level": result.risk_level,
    }
    with REMEDIATION_MEMORY_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, default=str))
        handle.write("\n")

    try:
        import chromadb
        from chromadb.api.types import Metadata
    except Exception:
        return

    text_payload = json.dumps(
        {
            "target_column": cluster.target_column,
            "anomaly_type": cluster.inferred_anomaly_type,
            "action": result.transformation_type,
            "reasoning": result.reasoning,
            "confidence": result.confidence_score,
        },
        ensure_ascii=True,
        default=str,
    )
    embedding = generate_embeddings([text_payload])
    if not embedding:
        return

    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection(name=REMEDIATION_MEMORY_COLLECTION)
        metadata: Metadata = {
            "memory_id": memory_id,
            "cluster_uid": cluster.cluster_uid,
            "pattern_key": cluster.pattern_key,
            "target_column": cluster.target_column,
            "inferred_anomaly_type": cluster.inferred_anomaly_type,
            "transformation_type": result.transformation_type,
            "confidence_score": result.confidence_score,
        }
        collection.upsert(ids=[memory_id], embeddings=[embedding[0]], metadatas=[metadata], documents=[text_payload])
    except Exception:
        return


def run(context: dict) -> dict:
    """Run Phase 3 remediation and return the updated pipeline context."""
    print("[Phase 3] Starting SLM remediation")
    updated_context = dict(context or {})
    raw_clusters = updated_context.get("clusters", [])

    if not isinstance(raw_clusters, list) or not raw_clusters:
        updated_context["remediations"] = []
        updated_context["phase3_status"] = "no_clusters"
        updated_context["phase3_summary"] = {
            "total": 0,
            "remediated": 0,
            "quarantined": 0,
            "requires_human_review": 0,
            "provider": get_phase3_provider(),
        }
        return updated_context

    remediations: list[dict[str, Any]] = []
    quarantined = 0
    requires_human_review = 0

    for raw_cluster in raw_clusters:
        if not isinstance(raw_cluster, dict):
            continue
        cluster = _normalize_cluster(raw_cluster)
        result = _remediate_cluster(cluster)
        if result.transformation_type == "quarantine":
            quarantined += 1
        if result.requires_human_review:
            requires_human_review += 1
        remediations.append(asdict(result))
        _record_remediation_memory(cluster, result)

    updated_context["remediations"] = remediations
    updated_context["phase3_status"] = "completed"
    updated_context["phase3_summary"] = {
        "total": len(remediations),
        "remediated": len(remediations) - quarantined,
        "quarantined": quarantined,
        "requires_human_review": requires_human_review,
        "provider": get_phase3_provider(),
    }
    return updated_context
