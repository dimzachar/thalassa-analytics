"""Config and helper functions for the LLM generation stack."""
from __future__ import annotations

import os
from typing import Literal

from dotenv import load_dotenv

load_dotenv()


LLM_PROVIDER: Literal["anthropic", "openrouter", "gemini"] = os.getenv("LLM_PROVIDER", "openrouter")

# Provider API keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

# Provider base URLs
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
GEMINI_BASE_URL = os.getenv("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")
ANTHROPIC_BASE_URL = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com/v1")


class ModelConfig:
    """Default model settings, overridable via environment variables."""

    GENERATION_MODEL = os.getenv("GENERATION_MODEL", os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free"))
    GENERATION_MAX_TOKENS = int(os.getenv("GENERATION_MAX_TOKENS", "2000"))
    GENERATION_TEMPERATURE = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))


MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_BASE = float(os.getenv("RETRY_DELAY_BASE", "1.0"))

LLM_TRACE_ENABLED = os.getenv("LLM_TRACE_ENABLED", "false").lower() == "true"
LLM_TRACE_HEAD_CHARS = int(os.getenv("LLM_TRACE_HEAD_CHARS", "500"))
LLM_TRACE_TAIL_CHARS = int(os.getenv("LLM_TRACE_TAIL_CHARS", "300"))

OPENROUTER_MODEL_MAP = {
    "gemini-2.0-flash-exp": "google/gemini-2.0-flash-exp",
    "gemini-2.0-flash-exp:free": "google/gemini-2.0-flash-exp:free",
    "gemini-2.5-flash": "google/gemini-2.5-flash",
    "gemini-2.5-flash-lite": "google/gemini-2.5-flash-lite",
    "claude-sonnet-4.5": "anthropic/claude-sonnet-4.5",
    "claude-sonnet-4.6": "anthropic/claude-sonnet-4.6",
    "openai/gpt-4.1-mini": "openai/gpt-4.1-mini",
    "google/gemini-2.0-flash-exp": "google/gemini-2.0-flash-exp",
    "google/gemini-2.0-flash-exp:free": "google/gemini-2.0-flash-exp:free",
    "google/gemini-2.5-flash": "google/gemini-2.5-flash",
}


def get_openrouter_model(model_name: str) -> str:
    """Translate a shorthand model name to its full OpenRouter identifier."""
    return OPENROUTER_MODEL_MAP.get(model_name, model_name)


def get_generation_model() -> str:
    """Return the model name to use for generation based on the active provider."""
    if LLM_PROVIDER == "gemini":
        return os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
    if LLM_PROVIDER == "anthropic":
        return os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
    return ModelConfig.GENERATION_MODEL


def validate_config() -> None:
    """Check that the active provider has its required API key set."""
    if LLM_PROVIDER == "anthropic" and not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY is required when LLM_PROVIDER=anthropic")
    if LLM_PROVIDER == "openrouter" and not OPENROUTER_API_KEY:
        raise ValueError("OPENROUTER_API_KEY is required when LLM_PROVIDER=openrouter")
    if LLM_PROVIDER == "gemini" and not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY is required when LLM_PROVIDER=gemini")
    if LLM_PROVIDER not in ["anthropic", "openrouter", "gemini"]:
        raise ValueError("Invalid LLM_PROVIDER. Must be 'anthropic', 'openrouter', or 'gemini'")


try:
    validate_config()
except ValueError as exc:
    print(f"Warning: Configuration validation failed: {exc}")
