"""
Reference file for Phase 3 local SLM wiring.

This file intentionally keeps only the commented local SLM/Ollama path so the
final repo can switch back to the intended architecture by uncommenting the
relevant sections.
"""

from __future__ import annotations

import os


# Local SLM wiring reference.
# Uncomment and adapt when moving from temporary API-backed testing to the
# intended local SLM runtime.
#
# OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").rstrip("/")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi3")
# OLLAMA_TIMEOUT_PING = int(os.getenv("OLLAMA_TIMEOUT_PING", "4"))
# OLLAMA_TIMEOUT_GEN = int(os.getenv("OLLAMA_TIMEOUT_GEN", "45"))
#
#
# def _requests_module():
#     try:
#         import requests
#     except Exception as exc:
#         raise RuntimeError(f"requests unavailable: {exc}") from exc
#     return requests
#
#
# def _ollama_alive() -> bool:
#     requests = _requests_module()
#     try:
#         response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=OLLAMA_TIMEOUT_PING)
#         return response.status_code == 200
#     except Exception:
#         return False
#
#
# def call_phase3_slm(prompt: str, system_prompt: str) -> tuple[str, str]:
#     requests = _requests_module()
#     payload = {
#         "model": OLLAMA_MODEL,
#         "prompt": prompt,
#         "system": system_prompt,
#         "stream": False,
#         "options": {"temperature": 0.0},
#     }
#     response = requests.post(
#         f"{OLLAMA_BASE_URL}/api/generate",
#         json=payload,
#         timeout=OLLAMA_TIMEOUT_GEN,
#     )
#     response.raise_for_status()
#     return response.json().get("response", "").strip(), f"ollama/{OLLAMA_MODEL}"
