"""
Module: EMBEDDING ENGINE
Owner: Aryan
Purpose:
- Convert anomaly rows into vector embeddings.
Responsibilities:
- Transform text records into embeddings.
- Prepare data for vector similarity search.
- Support clustering layer.
"""

from __future__ import annotations

import hashlib
from typing import Iterable

MODEL_NAME = "all-MiniLM-L6-v2"
_MODEL = None


def _load_model():
    """Lazily load the sentence-transformer model once per process."""
    global _MODEL
    if _MODEL is not None:
        return _MODEL
    try:
        from sentence_transformers import SentenceTransformer
    except Exception as exc:  # pragma: no cover - environment dependent
        print(f"[Embedding] sentence-transformers unavailable: {exc}")
        _MODEL = False
        return _MODEL
    _MODEL = SentenceTransformer(MODEL_NAME)
    return _MODEL


def _fallback_embedding(text: str, size: int = 32) -> list[float]:
    """Deterministic fallback vector to keep pipeline integration alive."""
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    repeated = (digest * ((size // len(digest)) + 1))[:size]
    return [byte / 255.0 for byte in repeated]


def generate_embeddings(texts: Iterable[str]) -> list[list[float]]:
    """
    Generate one embedding per input text.

    Uses SentenceTransformers(all-MiniLM-L6-v2) when available, otherwise
    falls back to deterministic hash embeddings for scaffold environments.
    """
    texts_list = list(texts)
    if not texts_list:
        return []

    model = _load_model()
    if model is False:  # pragma: no cover - environment dependent
        return [_fallback_embedding(text) for text in texts_list]

    vectors = model.encode(texts_list, convert_to_numpy=True, normalize_embeddings=True)
    return [vector.tolist() for vector in vectors]
