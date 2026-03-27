"""Handles LLM-based generation for the auto panel intelligence snapshot."""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime

from ..config import LLM_PROVIDER, MAX_RETRIES, RETRY_DELAY_BASE, get_generation_model
from ..llm_client import LLMClient
from ..models.schemas import (
    AutoPanelNarrativeOverlay,
    AutoPanelSourceSnapshot,
    GenerationMeta,
    SnapshotPayload,
)
from ..models.state import NotableChangeDecision
from .snapshot_agent import render_auto_panel_output


_OFF_DOMAIN_TERMS = {
    "security",
    "cyber",
    "breach",
    "login",
    "credential",
    "malware",
    "phishing",
    "firewall",
    "unauthorized",
    "outbound data",
    "port scan",
    "incident response",
    "intrusion",
    "threat actor",
}


def _build_prompt(source_snapshot: AutoPanelSourceSnapshot, reasons: list[str]) -> str:
    """Build the prompt sent to the LLM to request a narrative overlay."""
    payload = json.dumps(source_snapshot.model_dump(mode="json"), ensure_ascii=False)
    reason_text = "; ".join(reasons) if reasons else "no explicit trigger reason"
    valid_signal_refs = [delta.name for delta in source_snapshot.deltas] + [signal.kind for signal in source_snapshot.signals]
    action_candidates = [candidate.action for candidate in source_snapshot.action_candidates]
    return (
        "You are a senior maritime operations analyst for Greek ferry network reporting. "
        "You will receive a deterministic traffic snapshot in JSON. "
        "Use only the values, labels, signals, and action_candidates present in the snapshot. "
        "Do not invent external causes, weather, policy, or operational actions outside the snapshot. "
        "Preserve corridor and port names exactly as provided. No markdown.\n\n"
        "Return one JSON object with exactly these keys: "
        "headline, analyst_note, cited_signals.\n\n"
        "Task:\n"
        "1. Write a sharp headline.\n"
        "2. Put the analyst_note in 4-6 short paragraphs (1-2 sentences each), separated by blank lines.\n"
        "3. cited_signals must contain only exact names from the allowed signal refs list below.\n"
        "4. Keep the report about passenger traffic, corridors, ports, concentration, congestion, and operational actions only.\n\n"
        "5. End the final paragraph with a concrete next step phrased as a sentence (no 'Action:' label).\n\n"
        "Style rules:\n"
        "- The UI already shows the current period summary separately, so do not restate the full weekly totals, the exact current period label, or all headline metrics.\n"
        "- Do not start any paragraph with the current period label or a numeric recap.\n"
        "- Focus on interpretation: what changed, why it matters operationally, and what the shift implies.\n"
        "- Use at most two numeric references in analyst_note.\n"
        "- Avoid repeating the same fact twice across headline and analyst_note.\n\n"
        f"Trigger reasons: {reason_text}\n"
        f"Allowed signal refs: {json.dumps(valid_signal_refs, ensure_ascii=False)}\n"
        f"Action candidates (index aligned): {json.dumps(action_candidates, ensure_ascii=False)}\n"
        f"Snapshot JSON:\n{payload}"
    )


def _validate_overlay(overlay: AutoPanelNarrativeOverlay, source_snapshot: AutoPanelSourceSnapshot) -> None:
    """Check that the generated overlay only references signals and terms from the source snapshot."""
    valid_signal_refs = {delta.name for delta in source_snapshot.deltas} | {signal.kind for signal in source_snapshot.signals}
    invalid_refs = [signal_name for signal_name in overlay.cited_signals if signal_name not in valid_signal_refs]
    if invalid_refs:
        raise ValueError(f"Overlay cited invalid signals: {', '.join(invalid_refs)}")
    if not overlay.cited_signals:
        raise ValueError("Overlay must cite at least one snapshot signal")

    combined_text = " ".join([overlay.headline, overlay.analyst_note]).lower()
    invalid_terms = [term for term in _OFF_DOMAIN_TERMS if term in combined_text]
    if invalid_terms:
        raise ValueError(f"Overlay drifted off domain with terms: {', '.join(invalid_terms)}")

    domain_terms = {"passenger", "traffic", "corridor", "port", "route", "vehicle", "congestion", "demand", "volume"}
    top_entities = []
    if source_snapshot.top_corridors:
        top_entities.append(source_snapshot.top_corridors[0].pair_label.lower())
    if source_snapshot.top_ports:
        top_entities.append(source_snapshot.top_ports[0].port_name.lower())
    if not any(term in combined_text for term in domain_terms) and not any(entity in combined_text for entity in top_entities):
        raise ValueError("Overlay is too generic and does not reference maritime snapshot concepts")


def _normalize_overlay(overlay: AutoPanelNarrativeOverlay, source_snapshot: AutoPanelSourceSnapshot) -> AutoPanelNarrativeOverlay:
    """Clean up overlay content before it gets validated and rendered."""
    return overlay


def _provider_model_candidates() -> list[tuple[str, str]]:
    """Return the list of provider/model pairs to try, in priority order."""
    candidates = [
        (LLM_PROVIDER, get_generation_model()),
        ("openrouter", "google/gemini-2.0-flash-exp:free"),
        ("anthropic", os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")),
        ("gemini", os.getenv("GEMINI_MODEL", "gemini-1.5-flash")),
        ("openrouter", os.getenv("GENERATION_MODEL", os.getenv("OPENROUTER_MODEL", "google/gemini-2.0-flash-exp:free"))),
    ]

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for provider, model in candidates:
        key = (provider, model)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _generate_overlay(
    *,
    prompt: str,
    source_snapshot: AutoPanelSourceSnapshot,
) -> tuple[AutoPanelNarrativeOverlay, str]:
    """Try each provider in order to generate a valid narrative overlay, retrying on failure."""
    errors: list[str] = []

    for provider, model in _provider_model_candidates():
        try:
            client = LLMClient(provider=provider)
        except Exception as exc:
            errors.append(f"{provider}: {exc}")
            continue

        last_error: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if provider == "openrouter":
                    response = client.create_message(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=2000,
                        temperature=0.1,
                        system="Return only valid JSON.",
                    )
                    overlay = AutoPanelNarrativeOverlay.model_validate(
                        LLMClient._extract_json_object(str(response["content"]))
                    )
                else:
                    overlay = client.parse_message(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        output_model=AutoPanelNarrativeOverlay,
                        max_tokens=2000,
                        temperature=0.1,
                        system="Return only valid JSON.",
                    )
                overlay = _normalize_overlay(overlay, source_snapshot)
                _validate_overlay(overlay, source_snapshot)
                return overlay, f"{provider}:{model}"
            except Exception as exc:
                last_error = exc
                if attempt < MAX_RETRIES:
                    time.sleep(min(RETRY_DELAY_BASE * (2 ** (attempt - 1)), 8))

        errors.append(f"{provider}:{model}: {last_error}")

    raise RuntimeError(" | ".join(errors)[:500])


def generate_auto_panel_snapshot(
    *,
    data_version: str,
    source_snapshot: AutoPanelSourceSnapshot,
    notable_change: NotableChangeDecision,
) -> SnapshotPayload:
    """Build the final snapshot payload, using LLM output if a notable change was detected, otherwise deterministic output."""
    now_iso = datetime.now(UTC).isoformat()
    source_snapshot_json = json.dumps(source_snapshot.model_dump(mode="json"), ensure_ascii=False)

    if not notable_change.should_trigger_llm:
        output = render_auto_panel_output(source_snapshot=source_snapshot)
        return SnapshotPayload(
            chart_id="agent_panel",
            insight_type="auto_panel",
            grain="weekly",
            filter_hash=None,
            data_version=data_version,
            snapshot_ts_iso=now_iso,
            output=output,
            meta=GenerationMeta(generation_mode="deterministic_skip", generation_error=None, model_name=None),
            source_snapshot_json=source_snapshot_json,
            overlay_json=None,
        )

    try:
        prompt = _build_prompt(source_snapshot, notable_change.reasons)
        overlay, model_name = _generate_overlay(prompt=prompt, source_snapshot=source_snapshot)
        output = render_auto_panel_output(source_snapshot=source_snapshot, overlay=overlay)
        meta = GenerationMeta(generation_mode="llm_overlay", generation_error=None, model_name=model_name)
        overlay_json = json.dumps(overlay.model_dump(mode="json"), ensure_ascii=False)
    except Exception as exc:
        output = render_auto_panel_output(source_snapshot=source_snapshot)
        error_text = str(exc)[:500]
        generation_mode = "deterministic_no_api_key" if "not set" in error_text.lower() else "deterministic_llm_error"
        meta = GenerationMeta(generation_mode=generation_mode, generation_error=error_text, model_name=None)
        overlay_json = None

    return SnapshotPayload(
        chart_id="agent_panel",
        insight_type="auto_panel",
        grain="weekly",
        filter_hash=None,
        data_version=data_version,
        snapshot_ts_iso=now_iso,
        output=output,
        meta=meta,
        source_snapshot_json=source_snapshot_json,
        overlay_json=overlay_json,
    )
