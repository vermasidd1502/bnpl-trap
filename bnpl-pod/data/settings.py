"""Centralized settings — loads .env, exposes typed config to the whole pod."""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _env(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    return v if v else default


class Settings(BaseModel):
    # --- paths ---
    root: Path = ROOT
    duckdb_path: Path = ROOT / (os.getenv("DUCKDB_PATH", "data/warehouse.duckdb"))

    # --- LLMs ---
    nim_api_key: str | None = _env("NVIDIA_NIM_API_KEY")
    nim_base_url: str = _env("NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")
    nim_model_small: str = _env("NVIDIA_NIM_MODEL_SMALL", "nvidia/nemotron-mini-4b-instruct")
    nim_model_heavy: str = _env("NVIDIA_NIM_MODEL_HEAVY", "nvidia/nemotron-3-super-120b-a12b")
    gemini_api_key: str | None = _env("GEMINI_API_KEY")
    gemini_base_url: str = _env("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai/")
    gemini_model_small: str = _env("GEMINI_MODEL_SMALL", "gemini-2.5-flash")
    gemini_model_heavy: str = _env("GEMINI_MODEL_HEAVY", "gemini-2.5-pro")

    # --- data APIs ---
    fred_api_key: str | None = _env("FRED_API_KEY")
    sec_edgar_ua: str = _env("SEC_EDGAR_UA", "Siddharth Verma sverma24@illinois.edu")
    reddit_client_id: str | None = _env("REDDIT_CLIENT_ID")
    reddit_client_secret: str | None = _env("REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = _env("REDDIT_USER_AGENT", "bnpl-pod/0.1")

    # --- infra ---
    milvus_host: str = _env("MILVUS_HOST", "localhost")
    milvus_port: int = int(_env("MILVUS_PORT", "19530"))

    # --- pod flags ---
    # "" = auto failover (NIM primary, Gemini fallback). "nim" or "gemini" forces that provider.
    llm_force_provider: str = (_env("LLM_FORCE_PROVIDER", "") or "").lower()
    offline: bool = _env("POD_OFFLINE_MODE", "false").lower() == "true"


settings = Settings()


def load_weights() -> dict:
    with open(ROOT / "config" / "weights.yaml") as f:
        return yaml.safe_load(f)


def load_thresholds() -> dict:
    with open(ROOT / "config" / "thresholds.yaml") as f:
        return yaml.safe_load(f)
