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

import json
import logging
import math
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from utils.embedding_engine import generate_embeddings
from utils.temp_llm_test_phase_3 import call_phase3_provider, get_phase3_provider

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path("prompts")
REMEDIATION_PROMPT_FILE = PROMPTS_DIR / "remediation_v1.txt"
SYSTEM_RULES_FILE = PROMPTS_DIR / "system_rules.txt"
CONFIDENCE_THRESHOLD = 0.75

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
    sample_rows: list[Any]
    size: int
    member_ids: list[str]
    cache_hit: bool
    pattern_key: str
    inferred_anomaly_type: str = "unknown"
    sample_values: list[str] = field(default_factory=list)
    rule_context: dict[str, Any] = field(default_factory=dict)
    target_column: str = ""


@dataclass
class RemediationResult:
    cluster_id: str
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


def _build_rule_context(cluster: ClusterInput) -> dict[str, Any]:
    documents = _load_retrieval_documents()
    query_text = " ".join(
        [
            cluster.cluster_id,
            cluster.pattern_key,
            cluster.inferred_anomaly_type,
            cluster.target_column,
            *cluster.sample_values,
        ]
    ).strip()
    retrieved = _retrieve_rule_context(query_text, documents, top_k=3)
    return {
        "retrieved_rules": retrieved,
        "cluster_pattern_key": cluster.pattern_key,
        "cache_hit": cluster.cache_hit,
        "target_column": cluster.target_column,
    }


def _load_retrieval_documents() -> list[dict[str, str]]:
    documents: list[dict[str, str]] = []
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
                }
            )
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


def _retrieve_rule_context(query: str, documents: list[dict[str, str]], top_k: int = 3) -> list[dict[str, Any]]:
    if not query or not documents:
        return []

    scored_documents: list[dict[str, Any]] = []
    for document in documents:
        keyword_score = _keyword_overlap_score(query, document["text"])
        embedding_score = _embedding_similarity(query, document["text"])
        blended_score = round((0.35 * keyword_score) + (0.65 * embedding_score), 4)
        scored_documents.append(
            {
                "id": document["id"],
                "source": document["source"],
                "text": document["text"],
                "score": blended_score,
            }
        )

    scored_documents.sort(key=lambda item: item["score"], reverse=True)
    return scored_documents[:top_k]


def _build_user_prompt(cluster: ClusterInput) -> str:
    payload = {
        "cluster_id": cluster.cluster_id,
        "sample_values": cluster.sample_values,
        "anomaly_type": cluster.inferred_anomaly_type,
        "size": cluster.size,
        "pattern_key": cluster.pattern_key,
        "target_column": cluster.target_column,
        "rule_context": cluster.rule_context,
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


def _quarantine(cluster: ClusterInput, model_used: str, raw: str, reason: str) -> RemediationResult:
    return RemediationResult(
        cluster_id=cluster.cluster_id,
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
        raw_response=raw,
    )


def _parse_response(raw: str, cluster: ClusterInput, model_used: str) -> RemediationResult:
    try:
        clean = raw.strip()
        if clean.startswith("```"):
            clean = clean.strip("`").removeprefix("json").strip()
        data = json.loads(clean)
        confidence_score = float(data.get("confidence_score", 0.0))
        transformation_type = str(data.get("transformation_type", "rule"))
        if confidence_score < CONFIDENCE_THRESHOLD:
            return _quarantine(
                cluster,
                model_used,
                raw,
                f"Low confidence remediation ({confidence_score:.2f}).",
            )

        return RemediationResult(
            cluster_id=cluster.cluster_id,
            transformation_type=transformation_type,
            code=str(data.get("code", "lambda x: x")),
            confidence_score=confidence_score,
            reasoning=str(data.get("reasoning", "")),
            fallback_value=str(data.get("fallback_value", "null")),
            inferred_anomaly_type=cluster.inferred_anomaly_type,
            model_used=model_used,
            member_ids=cluster.member_ids,
            size=cluster.size,
            cache_hit=cluster.cache_hit,
            pattern_key=cluster.pattern_key,
            raw_response=raw,
        )
    except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        return _quarantine(cluster, model_used, raw, f"Parse error: {exc}")


def _normalize_cluster(raw_cluster: dict[str, Any]) -> ClusterInput:
    sample_rows = raw_cluster.get("sample_rows", [])
    sample_values = _extract_sample_values(sample_rows)
    target_column = ""
    if sample_rows and isinstance(sample_rows, list):
        first_row = next((row for row in sample_rows if isinstance(row, dict) and row), None)
        if isinstance(first_row, dict):
            target_column = str(first_row.get("column_name") or first_row.get("field") or next(iter(first_row.keys()), ""))
    cluster = ClusterInput(
        cluster_id=str(raw_cluster["cluster_id"]),
        sample_rows=sample_rows if isinstance(sample_rows, list) else [],
        size=int(raw_cluster.get("size", 0)),
        member_ids=[str(member_id) for member_id in raw_cluster.get("member_ids", [])],
        cache_hit=bool(raw_cluster.get("cache_hit", False)),
        pattern_key=str(raw_cluster.get("pattern_key", "")),
        sample_values=sample_values,
        target_column=target_column,
    )
    cluster.inferred_anomaly_type = _infer_anomaly_type(cluster.sample_values)
    cluster.rule_context = _build_rule_context(cluster)
    return cluster


def _remediate_cluster(cluster: ClusterInput) -> RemediationResult:
    try:
        raw, model_used = _call_provider(cluster)
    except Exception as exc:
        logger.error("[Phase 3] Provider failed for %s: %s", cluster.cluster_id, exc)
        return _quarantine(cluster, "none", "", f"Provider failure: {exc}")
    return _parse_response(raw, cluster, model_used)


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
            "provider": get_phase3_provider(),
        }
        return updated_context

    remediations: list[dict[str, Any]] = []
    quarantined = 0

    for raw_cluster in raw_clusters:
        if not isinstance(raw_cluster, dict):
            continue
        cluster = _normalize_cluster(raw_cluster)
        result = _remediate_cluster(cluster)
        if result.transformation_type == "quarantine":
            quarantined += 1
        remediations.append(asdict(result))

    updated_context["remediations"] = remediations
    updated_context["phase3_status"] = "completed"
    updated_context["phase3_summary"] = {
        "total": len(remediations),
        "remediated": len(remediations) - quarantined,
        "quarantined": quarantined,
        "provider": get_phase3_provider(),
    }
    return updated_context
