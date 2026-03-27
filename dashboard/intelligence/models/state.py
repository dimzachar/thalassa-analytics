"""Dataclass state containers used by the dashboard intelligence layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class AgentInsightSnapshot:
    """Cached snapshot content and metadata for a single dashboard card."""
    title: str
    lines: list[str]
    generated_at: str | None = None
    generation_mode: str | None = None
    model_name: str | None = None
    scope_label: str | None = None
    kpi_start: str | None = None
    kpi_end: str | None = None


@dataclass
class SnapshotFreshness:
    """Freshness information for the latest available snapshot."""
    latest_snapshot_ts: datetime | None
    hours_since_latest: float | None
    is_stale: bool


@dataclass
class NotableChangeDecision:
    """Decision record describing whether the current scope merits LLM generation."""
    should_trigger_llm: bool
    reasons: list[str]


@dataclass
class AgentContext:
    """Aggregated intelligence state used to render the dashboard view."""
    filter_hash: str
    data_version: str
    panel_snapshot: AgentInsightSnapshot | None
    freshness: SnapshotFreshness
    notable_change: NotableChangeDecision
    deepen_traffic_pulse_snapshot: AgentInsightSnapshot | None
    deepen_top_corridors_snapshot: AgentInsightSnapshot | None
    deepen_port_balance_snapshot: AgentInsightSnapshot | None
