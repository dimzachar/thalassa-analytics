"""Single client that talks to Anthropic, OpenRouter, or Gemini."""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, TypeVar

import requests
from pydantic import BaseModel

from .config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_BASE_URL,
    GEMINI_BASE_URL,
    GOOGLE_API_KEY,
    LLM_PROVIDER,
    LLM_TRACE_ENABLED,
    LLM_TRACE_HEAD_CHARS,
    LLM_TRACE_TAIL_CHARS,
    MAX_RETRIES,
    OPENROUTER_API_KEY,
    OPENROUTER_BASE_URL,
    RETRY_DELAY_BASE,
    get_generation_model,
    get_openrouter_model,
)

StructuredResponseT = TypeVar("StructuredResponseT", bound=BaseModel)


class LLMClient:
    """Talks to Anthropic, OpenRouter, or Gemini depending on config."""

    OPENROUTER_JSON_PLUGIN = {"id": "response-healing"}

    def __init__(self, provider: Optional[str] = None):
        """Set up the client for the given provider and check that the API key is present."""
        self.provider = provider or LLM_PROVIDER
        if self.provider == "anthropic" and not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set")
        if self.provider == "openrouter" and not OPENROUTER_API_KEY:
            raise ValueError("OPENROUTER_API_KEY not set")
        if self.provider == "gemini" and not GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY not set")
        if self.provider not in {"anthropic", "openrouter", "gemini"}:
            raise ValueError(f"Unknown provider: {self.provider}")

    @staticmethod
    def _normalize_text_content(content: Any) -> str:
        """Turn whatever the provider returned as content into a plain string."""
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = item.get("text") or item.get("content")
                    if isinstance(text, str):
                        parts.append(text)
            return "\n".join(part for part in parts if part).strip()
        if isinstance(content, dict):
            text = content.get("text") or content.get("content")
            if isinstance(text, str):
                return text
            return json.dumps(content, ensure_ascii=False)
        return str(content)

    @classmethod
    def _preview_text(cls, text: Any) -> str:
        """Return a short preview of text for trace logging, trimming the middle if it's too long."""
        normalized = cls._normalize_text_content(text).strip()
        if not normalized:
            return "<empty>"
        visible_chars = max(0, LLM_TRACE_HEAD_CHARS) + max(0, LLM_TRACE_TAIL_CHARS)
        if visible_chars <= 0 or len(normalized) <= visible_chars:
            return normalized
        head = normalized[: max(0, LLM_TRACE_HEAD_CHARS)]
        tail = normalized[-max(0, LLM_TRACE_TAIL_CHARS) :] if LLM_TRACE_TAIL_CHARS > 0 else ""
        omitted = len(normalized) - len(head) - len(tail)
        return f"{head}\n... [truncated {omitted} chars] ...\n{tail}".strip()

    @classmethod
    def _emit_trace(cls, header: str, body: str) -> None:
        """Print a trace block to stdout if tracing is turned on."""
        if LLM_TRACE_ENABLED:
            print(f"\n=== {header} ===\n{body}\n=== END {header} ===", flush=True)

    @classmethod
    def _trace_request(
        cls,
        provider: str,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        system: Optional[str],
    ) -> None:
        """Log the details of an outgoing LLM request."""
        sections: List[str] = [
            f"provider={provider}",
            f"model={model}",
            f"max_tokens={max_tokens}",
            f"temperature={temperature}",
            f"message_count={len(messages)}",
        ]
        if system is not None:
            sections.append(f"system:\n{cls._preview_text(system)}")
        for index, message in enumerate(messages, start=1):
            sections.append(f"[{index}:{message.get('role', 'unknown')}]\n{cls._preview_text(message.get('content'))}")
        cls._emit_trace("LLM REQUEST", "\n".join(sections))

    @classmethod
    def _trace_response(cls, model: str, usage: Dict[str, Any], content: Any) -> None:
        """Log the details of an incoming LLM response."""
        cls._emit_trace(
            "LLM RESPONSE",
            "\n".join(
                [
                    f"model={model}",
                    f"usage={json.dumps(usage, ensure_ascii=False)}",
                    "content:",
                    cls._preview_text(content),
                ]
            ),
        )

    @staticmethod
    def _build_chat_messages(messages: List[Dict[str, Any]], system: Optional[str] = None) -> List[Dict[str, str]]:
        """Build a list of chat messages, adding the system prompt at the top if provided."""
        chat_messages: List[Dict[str, str]] = []
        if system:
            chat_messages.append({"role": "system", "content": system})
        for message in messages:
            chat_messages.append({"role": message["role"], "content": str(message["content"])})
        return chat_messages

    @staticmethod
    def _sanitize_json_schema(value: Any) -> Any:
        """Strip out schema fields that some providers don't support."""
        if isinstance(value, dict):
            cleaned = {
                key: LLMClient._sanitize_json_schema(subvalue)
                for key, subvalue in value.items()
                if key != "default"
            }
            if cleaned.get("type") == "object":
                cleaned.setdefault("additionalProperties", False)
            return cleaned
        if isinstance(value, list):
            return [LLMClient._sanitize_json_schema(item) for item in value]
        return value

    @classmethod
    def _build_structured_schema(cls, output_model: type[BaseModel]) -> Dict[str, Any]:
        """Wrap a Pydantic model's JSON schema in the format providers expect for structured output."""
        return {
            "type": "json_schema",
            "json_schema": {
                "name": output_model.__name__,
                "strict": True,
                "schema": cls._sanitize_json_schema(output_model.model_json_schema()),
            },
        }

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        """Pull the first JSON object out of a raw model response string."""
        stripped = text.strip()
        if stripped.startswith("```"):
            stripped = stripped.strip("`")
            if stripped.lower().startswith("json"):
                stripped = stripped[4:].strip()
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("No JSON object detected in LLM output")
        return json.loads(stripped[start : end + 1])

    def _create_openrouter_message(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        system: Optional[str] = None,
        expect_json: bool = False,
    ) -> Dict[str, Any]:
        """Send a request to OpenRouter and return the response as a plain dict."""
        openrouter_model = get_openrouter_model(model)
        payload: Dict[str, Any] = {
            "model": openrouter_model,
            "messages": self._build_chat_messages(messages, system),
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if expect_json:
            payload["plugins"] = [self.OPENROUTER_JSON_PLUGIN]

        response = requests.post(
            url=f"{OPENROUTER_BASE_URL.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        message = data["choices"][0]["message"]
        content = self._normalize_text_content(message.get("content"))
        return {
            "content": content,
            "model": data.get("model", openrouter_model),
            "usage": data.get("usage", {}),
        }

    def _create_anthropic_message(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        system: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a request to Anthropic and return the response as a plain dict."""
        payload: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": message["role"], "content": str(message["content"])} for message in messages],
        }
        if system:
            payload["system"] = system

        response = requests.post(
            url=f"{ANTHROPIC_BASE_URL.rstrip('/')}/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            data=json.dumps(payload),
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        content = "".join(
            str(item.get("text", ""))
            for item in data.get("content", [])
            if isinstance(item, dict)
        ).strip()
        return {
            "content": content,
            "model": data.get("model", model),
            "usage": data.get("usage", {}),
        }

    def _create_gemini_message(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        system: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a request to Gemini and return the response as a plain dict."""
        prompt_parts: List[str] = []
        if system:
            prompt_parts.append(system)
        for message in messages:
            role = message["role"].upper()
            prompt_parts.append(f"{role}: {message['content']}")
        prompt_parts.append("Return only the final answer.")

        payload = {
            "contents": [{"parts": [{"text": "\n\n".join(prompt_parts)}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }

        response = requests.post(
            url=f"{GEMINI_BASE_URL.rstrip('/')}/models/{model}:generateContent?key={GOOGLE_API_KEY}",
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        candidates = data.get("candidates", [])
        parts = []
        for candidate in candidates:
            content = candidate.get("content", {})
            for part in content.get("parts", []):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return {
            "content": "\n".join(parts).strip(),
            "model": model,
            "usage": data.get("usageMetadata", {}),
        }

    def create_message(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        max_tokens: int = 4000,
        temperature: float = 0.2,
        system: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatch a freeform text request to the configured provider."""
        self._trace_request(self.provider, model, messages, max_tokens, temperature, system)
        if self.provider == "anthropic":
            response = self._create_anthropic_message(model, messages, max_tokens, temperature, system)
        elif self.provider == "gemini":
            response = self._create_gemini_message(model, messages, max_tokens, temperature, system)
        else:
            response = self._create_openrouter_message(model, messages, max_tokens, temperature, system)
        self._trace_response(response["model"], response.get("usage", {}), response.get("content"))
        return response

    def parse_message(
        self,
        model: str,
        messages: List[Dict[str, Any]],
        output_model: type[StructuredResponseT],
        max_tokens: int = 4000,
        temperature: float = 0.2,
        system: Optional[str] = None,
    ) -> StructuredResponseT:
        """Request structured output and repair malformed JSON when needed."""
        raw_content: str = ""
        try:
            if self.provider == "openrouter":
                response = self._create_openrouter_message(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=system,
                    expect_json=True,
                )
            else:
                schema = json.dumps(output_model.model_json_schema(), ensure_ascii=False)
                structured_system = (
                    f"{system}\n\nReturn exactly one valid JSON object matching this schema. "
                    f"Do not add markdown or commentary.\nSchema:\n{schema}"
                    if system
                    else f"Return exactly one valid JSON object matching this schema. "
                    f"Do not add markdown or commentary.\nSchema:\n{schema}"
                )
                response = self.create_message(
                    model=model,
                    messages=messages,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system=structured_system,
                )

            raw_content = str(response.get("content", ""))
            parsed = output_model.model_validate(self._extract_json_object(raw_content))
            self._trace_response(response["model"], response.get("usage", {}), parsed.model_dump_json(indent=2))
            return parsed
        except Exception:
            schema = json.dumps(output_model.model_json_schema(), ensure_ascii=False)
            repair_prompt = (
                "Reformat the following model output into one valid JSON object that matches this schema exactly. "
                "Do not add explanation, markdown, or code fences.\n\n"
                f"Schema:\n{schema}\n\n"
                f"Original output:\n{raw_content}"
            )
            repaired = self.create_message(
                model=model,
                messages=[{"role": "user", "content": repair_prompt}],
                max_tokens=max_tokens,
                temperature=0.0,
                system="You are a JSON formatter. Return only valid JSON.",
            )
            return output_model.model_validate(self._extract_json_object(repaired["content"]))


_client: Optional[LLMClient] = None


def get_llm_client() -> LLMClient:
    """Return a cached default LLM client instance."""
    global _client
    if _client is None:
        _client = LLMClient()
    return _client


def call_llm_with_retry(
    *,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_retries: int = MAX_RETRIES,
) -> str:
    """Call the active generation model with retries and return plain text output."""
    client = get_llm_client()
    model = get_generation_model()
    for attempt in range(1, max_retries + 1):
        try:
            response = client.create_message(
                model=model,
                messages=[{"role": "user", "content": user_prompt}],
                max_tokens=2000,
                temperature=temperature,
                system=system_prompt,
            )
            return str(response["content"])
        except Exception:
            if attempt >= max_retries:
                raise
            time.sleep(min(RETRY_DELAY_BASE * (2 ** (attempt - 1)), 8))
    raise RuntimeError("LLM retry loop exhausted")


def call_structured_with_retry(
    *,
    system_prompt: str,
    user_prompt: str,
    output_model: type[StructuredResponseT],
    temperature: float = 0.2,
    max_retries: int = MAX_RETRIES,
) -> StructuredResponseT:
    """Call the active generation model for structured output with retry and repair logic."""
    client = get_llm_client()
    model = get_generation_model()
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            return client.parse_message(
                model=model,
                messages=[{"role": "user", "content": user_prompt}],
                output_model=output_model,
                max_tokens=2000,
                temperature=temperature,
                system=system_prompt,
            )
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(min(RETRY_DELAY_BASE * (2 ** (attempt - 1)), 8))

    # After retries exhausted, do one raw generation + explicit repair pass.
    if last_error is not None:
        raw = call_llm_with_retry(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            max_retries=1,
        )
        schema = json.dumps(output_model.model_json_schema(), ensure_ascii=False)
        repair_prompt = (
            "Reformat the following model output into one valid JSON object that matches this schema exactly. "
            "Do not add explanation, markdown, or code fences.\n\n"
            f"Schema:\n{schema}\n\n"
            f"Original output:\n{raw}"
        )
        repaired = call_llm_with_retry(
            system_prompt="You are a JSON formatter. Return only valid JSON.",
            user_prompt=repair_prompt,
            temperature=0.0,
            max_retries=1,
        )
        return output_model.model_validate(LLMClient._extract_json_object(repaired))

    raise RuntimeError("LLM retry loop exhausted")
