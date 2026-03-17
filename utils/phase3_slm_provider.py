"""
Local Phase 3 SLM provider.

This module keeps Phase 3 aligned with the local-first architecture:
- `mock` provider for deterministic tests
- `ollama` provider for the actual local SLM runtime
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#") or "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip().lstrip("`")
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(ENV_FILE)


def get_phase3_provider() -> str:
    provider = os.getenv("PHASE3_PROVIDER", "ollama").strip().lower()
    if provider not in {"mock", "ollama"}:
        return "ollama"
    return provider


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "60"))


def _requests_module():
    try:
        import requests
    except Exception as exc:
        raise RuntimeError(f"requests unavailable: {exc}") from exc
    return requests


def _call_ollama(prompt: str, system_prompt: str) -> tuple[str, str]:
    requests = _requests_module()
    schema = {
        "type": "object",
        "properties": {
            "transformation_type": {"type": "string"},
            "code": {"type": "string"},
            "confidence_score": {"type": "number"},
            "reasoning": {"type": "string"},
            "fallback_value": {"type": "string"},
        },
        "required": [
            "transformation_type",
            "code",
            "confidence_score",
            "reasoning",
            "fallback_value",
        ],
    }
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "format": schema,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "options": {
            "temperature": 0.0,
            "top_p": 0.9,
            "num_predict": 768,
        },
    }
    response = requests.post(
        f"{OLLAMA_URL.rstrip('/')}/api/chat",
        json=payload,
        timeout=OLLAMA_TIMEOUT,
    )
    response.raise_for_status()
    body = response.json()
    message = body.get("message", {}) if isinstance(body, dict) else {}
    content = message.get("content", "") if isinstance(message, dict) else ""
    if not str(content).strip():
        raise RuntimeError("Ollama returned empty content.")
    return str(content).strip(), f"ollama/{OLLAMA_MODEL}"


def call_phase3_provider(
    *,
    prompt: str,
    system_prompt: str,
    cluster: Any,
    mock_response_builder: Callable[[Any], tuple[str, str]],
) -> tuple[str, str]:
    provider = get_phase3_provider()

    if provider == "mock":
        return mock_response_builder(cluster)
    return _call_ollama(prompt, system_prompt)
