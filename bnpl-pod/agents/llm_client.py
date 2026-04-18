"""
Unified LLM client — NVIDIA NIM primary, Google Gemini fallback.

Both providers expose OpenAI-compatible `/v1/chat/completions` endpoints, so
a single `openai.OpenAI` client class is used for both — only the base_url
and model name change. This is the cleanest possible fallback design: same
request shape, same response shape, no adapter code.

All LLM calls in the pod MUST go through `LLMClient.chat()`. This:
  - Routes small-tier vs heavy-tier by declared `tier`.
  - Fails over from NIM -> Gemini on any exception (5xx, timeout, rate limit).
  - Honors `LLM_FORCE_PROVIDER={nim,gemini}` env for debugging.
  - Logs every call (prompt hash, provider, model, latency, tokens) to
    logs/agent_decisions/YYYY-MM-DD.jsonl for audit.
  - Is deterministic-friendly: temperature=0 by default.

IMPORTANT — this client is for ADVISORY reasoning only. The deterministic
compliance engine (agents.compliance_engine) is the sole approval authority.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from openai import OpenAI   # Works for BOTH NIM and Gemini (OpenAI-compatible endpoints).

from data.settings import settings

Tier = Literal["small", "heavy"]
Provider = Literal["nim", "gemini"]

LOG_DIR = Path(__file__).resolve().parent.parent / "logs" / "agent_decisions"
LOG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class LLMResponse:
    text: str
    provider: Provider
    model: str
    latency_ms: int
    prompt_hash: str
    meta: dict = field(default_factory=dict)


class LLMProviderUnavailable(RuntimeError):
    """Raised when all configured providers have failed on a single call."""


class LLMClient:
    def __init__(self) -> None:
        self._nim = (
            OpenAI(api_key=settings.nim_api_key, base_url=settings.nim_base_url)
            if settings.nim_api_key else None
        )
        self._gemini = (
            OpenAI(api_key=settings.gemini_api_key, base_url=settings.gemini_base_url)
            if settings.gemini_api_key else None
        )

    # ---- public API ---------------------------------------------------------
    def chat(
        self,
        system: str,
        user: str,
        tier: Tier = "small",
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> LLMResponse:
        prompt_hash = _hash(system + "\n---\n" + user)

        # Resolve provider order based on force flag + availability.
        order: list[Provider]
        forced = settings.llm_force_provider
        if forced == "nim":
            order = ["nim"]
        elif forced == "gemini":
            order = ["gemini"]
        else:
            order = ["nim", "gemini"]   # NIM primary, Gemini fallback

        errors: dict[Provider, str] = {}
        for prov in order:
            try:
                return self._dispatch(prov, system, user, tier, temperature, max_tokens, prompt_hash)
            except Exception as e:   # noqa: BLE001 — failover on any provider error
                errors[prov] = repr(e)
                self._log(prompt_hash, prov, "<failed>", 0, {"error": repr(e)[:300]})

        raise LLMProviderUnavailable(
            f"All providers failed for prompt {prompt_hash}: {errors}"
        )

    # ---- provider dispatch --------------------------------------------------
    def _dispatch(self, prov: Provider, system: str, user: str, tier: Tier,
                  temperature: float, max_tokens: int, prompt_hash: str) -> LLMResponse:
        if prov == "nim":
            client, model = self._resolve_nim(tier)
        elif prov == "gemini":
            client, model = self._resolve_gemini(tier)
        else:
            raise ValueError(f"unknown provider: {prov}")

        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system},
                      {"role": "user",   "content": user}],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        latency_ms = int((time.perf_counter() - t0) * 1000)
        text = resp.choices[0].message.content or ""
        tokens = getattr(resp, "usage", None) and resp.usage.total_tokens
        meta = {"tokens": tokens, "finish_reason": resp.choices[0].finish_reason}
        self._log(prompt_hash, prov, model, latency_ms, meta)
        return LLMResponse(text=text, provider=prov, model=model,
                           latency_ms=latency_ms, prompt_hash=prompt_hash, meta=meta)

    def _resolve_nim(self, tier: Tier) -> tuple[OpenAI, str]:
        if self._nim is None:
            raise RuntimeError("NVIDIA_NIM_API_KEY is not set")
        model = settings.nim_model_heavy if tier == "heavy" else settings.nim_model_small
        return self._nim, model

    def _resolve_gemini(self, tier: Tier) -> tuple[OpenAI, str]:
        if self._gemini is None:
            raise RuntimeError("GEMINI_API_KEY is not set")
        model = settings.gemini_model_heavy if tier == "heavy" else settings.gemini_model_small
        return self._gemini, model

    # ---- audit log ----------------------------------------------------------
    @staticmethod
    def _log(prompt_hash: str, provider: str, model: str, latency_ms: int, meta: dict) -> None:
        row = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            "prompt_hash": prompt_hash,
            "provider": provider,
            "model": model,
            "latency_ms": latency_ms,
            "meta": meta,
        }
        day = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        with open(LOG_DIR / f"{day}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:16]
