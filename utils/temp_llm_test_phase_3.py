"""
Temporary LLM test provider for Phase 3.

This module isolates non-final API-backed testing logic from the main Phase 3
remediation code so it can be removed cleanly before the final push.
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
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(ENV_FILE)


def get_phase3_provider() -> str:
    return os.getenv("PHASE3_PROVIDER", "mock").strip().lower()


GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "30"))

SARVAM_API_URL = "https://api.sarvam.ai/v1/chat/completions"
SARVAM_API_KEY = os.getenv("SARVAM_API_KEY", "")
SARVAM_MODEL = os.getenv("SARVAM_MODEL", "sarvam-m")
SARVAM_TIMEOUT = int(os.getenv("SARVAM_TIMEOUT", "30"))


def _requests_module():
    try:
        import requests
    except Exception as exc:
        raise RuntimeError(f"requests unavailable: {exc}") from exc
    return requests


def _extract_gemini_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        return ""

    content = candidates[0].get("content", {})
    parts = content.get("parts", [])
    if not isinstance(parts, list):
        return ""

    chunks: list[str] = []
    for part in parts:
        if isinstance(part, dict) and "text" in part:
            chunks.append(str(part["text"]))
    return "".join(chunks).strip()


def _call_gemini(prompt: str, system_prompt: str) -> tuple[str, str]:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not set.")
    requests = _requests_module()
    headers = {
        "x-goog-api-key": GEMINI_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "system_instruction": {
            "parts": [{"text": system_prompt}]
        },
        "contents": [
            {
                "parts": [{"text": prompt}]
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 512,
        },
    }
    response = requests.post(
        f"{GEMINI_API_URL}/{GEMINI_MODEL}:generateContent",
        json=payload,
        headers=headers,
        timeout=GEMINI_TIMEOUT,
    )
    response.raise_for_status()
    body = response.json()
    return _extract_gemini_text(body), f"gemini/{GEMINI_MODEL}"


# Temporary Sarvam implementation reference.
# Keep this block commented so Gemini stays the active provider.
#
# def _call_sarvam(prompt: str, system_prompt: str) -> tuple[str, str]:
#     if not SARVAM_API_KEY:
#         raise RuntimeError("SARVAM_API_KEY is not set.")
#     requests = _requests_module()
#     headers = {
#         "Authorization": f"Bearer {SARVAM_API_KEY}",
#         "Content-Type": "application/json",
#     }
#     payload = {
#         "model": SARVAM_MODEL,
#         "messages": [
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": prompt},
#         ],
#         "temperature": 0.0,
#         "max_tokens": 512,
#     }
#     response = requests.post(
#         SARVAM_API_URL,
#         json=payload,
#         headers=headers,
#         timeout=SARVAM_TIMEOUT,
#     )
#     response.raise_for_status()
#     return response.json()["choices"][0]["message"]["content"].strip(), f"sarvam/{SARVAM_MODEL}"


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
    if provider == "gemini":
        return _call_gemini(prompt, system_prompt)
    if provider == "sarvam":
        raise RuntimeError("Sarvam path is commented out in this build. Switch PHASE3_PROVIDER or uncomment the Sarvam block.")
    if provider == "ollama":
        raise RuntimeError("Local SLM path is stored in utils/phase3_slm_provider.py and is disabled in this build.")

    return mock_response_builder(cluster)
