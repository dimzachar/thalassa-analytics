from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from typing import Any

import requests

from .detector import NotableChangeDecision
from .models import AutoPanelOutput, GenerationMeta, SnapshotPayload


def _deterministic_output(context: dict[str, Any]) -> AutoPanelOutput:
    return AutoPanelOutput(
        title="Harbor Intelligence",
        summary=context.get("summary", "No summary available."),
        facts=context.get("facts", []),
        anomalies=context.get("anomalies", []),
        comparisons=context.get("comparisons", []),
        recommendations=context.get("recommendations", []),
        report_text=context.get("report_text", context.get("summary", "No report available.")),
        insight_text=None,
    )


def _build_prompt(context: dict[str, Any], reasons: list[str]) -> str:
    payload = json.dumps(context, ensure_ascii=False)
    reason_text = "; ".join(reasons) if reasons else "no explicit trigger reason"
    return (
        "You are a maritime operations analyst. Return STRICT JSON with keys: "
        "title, summary, facts, anomalies, comparisons, recommendations, report_text, insight_text. "
        "Use exact values from context. No markdown. Keep concise and factual. "
        f"Trigger reasons: {reason_text}. Context: {payload}"
    )


def _call_llm_with_retry(prompt: str, *, model_name: str, api_key: str, api_url: str, max_retries: int = 3) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": "Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(api_url, headers=headers, json=body, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            return str(content)
        except Exception:
            if attempt >= max_retries:
                raise
            time.sleep(min(2 ** attempt, 8))

    raise RuntimeError("LLM retry loop exhausted")


def _extract_json(text: str) -> dict[str, Any]:
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


def generate_auto_panel_snapshot(
    *,
    data_version: str,
    context: dict[str, Any],
    notable_change: NotableChangeDecision,
) -> SnapshotPayload:
    now_iso = datetime.now(UTC).isoformat()
    model_name = os.getenv("THALASSA_LLM_MODEL", "gpt-4.1-mini")
    api_key = os.getenv("THALASSA_LLM_API_KEY", "").strip()
    api_url = os.getenv("THALASSA_LLM_API_URL", "https://api.openai.com/v1/chat/completions").strip()

    if not notable_change.should_trigger_llm:
        output = _deterministic_output(context)
        return SnapshotPayload(
            chart_id="agent_panel",
            insight_type="auto_panel",
            grain="weekly",
            filter_hash=None,
            data_version=data_version,
            snapshot_ts_iso=now_iso,
            output=output,
            meta=GenerationMeta(generation_mode="deterministic_skip", generation_error=None, model_name=None),
        )

    if not api_key:
        output = _deterministic_output(context)
        return SnapshotPayload(
            chart_id="agent_panel",
            insight_type="auto_panel",
            grain="weekly",
            filter_hash=None,
            data_version=data_version,
            snapshot_ts_iso=now_iso,
            output=output,
            meta=GenerationMeta(generation_mode="deterministic_no_api_key", generation_error="Missing THALASSA_LLM_API_KEY", model_name=None),
        )

    try:
        prompt = _build_prompt(context, notable_change.reasons)
        raw = _call_llm_with_retry(prompt, model_name=model_name, api_key=api_key, api_url=api_url)
        parsed = _extract_json(raw)
        output = AutoPanelOutput.model_validate(parsed)
        meta = GenerationMeta(generation_mode="llm", generation_error=None, model_name=model_name)
    except Exception as exc:
        output = _deterministic_output(context)
        meta = GenerationMeta(generation_mode="deterministic_llm_error", generation_error=str(exc)[:500], model_name=model_name)

    return SnapshotPayload(
        chart_id="agent_panel",
        insight_type="auto_panel",
        grain="weekly",
        filter_hash=None,
        data_version=data_version,
        snapshot_ts_iso=now_iso,
        output=output,
        meta=meta,
    )
