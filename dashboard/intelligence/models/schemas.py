"""Pydantic schemas for structured intelligence snapshots, overlays, and payloads."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, Field, field_validator


def _truncate(n: int):
    """Return a validator that truncates incoming lists to a fixed maximum length."""
    def _inner(v: object) -> list:
        """Trim list inputs to the configured maximum length."""
        if isinstance(v, list):
            return v[:n]
        return v
    return BeforeValidator(_inner)


class AutoPanelScope(BaseModel):
    """Time-window labels describing the baseline and KPI periods."""
    baseline_start: str
    baseline_end: str
    kpi_start: str
    kpi_end: str
    baseline_label: str = Field(min_length=3, max_length=120)
    kpi_label: str = Field(min_length=3, max_length=120)
    scope_label: str = Field(min_length=3, max_length=160)


class AutoPanelKpis(BaseModel):
    """Core KPI values used to summarize the scoped reporting window."""
    passengers: int = Field(ge=0)
    vehicles: int = Field(ge=0)
    traffic_rows: int = Field(ge=0)
    active_corridors: int = Field(ge=0)
    congestion_index_pct: float = Field(ge=0, le=100)
    recent_7d_avg_passengers: float = Field(ge=0)
    prior_7d_avg_passengers: float | None = Field(default=None, ge=0)
    recent_30d_avg_passengers: float | None = Field(default=None, ge=0)
    prior_30d_avg_passengers: float | None = Field(default=None, ge=0)


class AutoPanelCorridorMetric(BaseModel):
    """Passenger and vehicle totals for a ranked corridor."""
    pair_label: str = Field(min_length=3, max_length=160)
    passengers: int = Field(ge=0)
    vehicles: int = Field(ge=0)
    share_pct: float = Field(ge=0, le=100)


class AutoPanelPortMetric(BaseModel):
    """Passenger total for a ranked port."""
    port_name: str = Field(min_length=1, max_length=120)
    passengers: int = Field(ge=0)


class AutoPanelDelta(BaseModel):
    """Named comparison between the current window and a prior baseline window."""
    name: str = Field(min_length=3, max_length=80)
    label: str = Field(min_length=3, max_length=120)
    period: str = Field(min_length=3, max_length=80)
    direction: Literal["up", "down", "flat", "unknown"]
    severity: Literal["info", "watch", "alert"]
    value_pct: float | None = None
    summary: str = Field(min_length=3, max_length=240)


class AutoPanelSignal(BaseModel):
    """Derived signal describing a noteworthy operational condition."""
    kind: str = Field(min_length=3, max_length=80)
    severity: Literal["info", "watch", "alert"]
    summary: str = Field(min_length=3, max_length=240)


class AutoPanelActionCandidate(BaseModel):
    """Suggested follow-up action derived from the current snapshot signals."""
    action: str = Field(min_length=3, max_length=200)
    rationale: str = Field(min_length=3, max_length=240)
    priority: Literal["monitor", "investigate", "escalate"]


class AutoPanelSourceSnapshot(BaseModel):
    """Structured deterministic input used for fallback and LLM-generated reports."""
    schema_version: Literal["auto_panel_source_v2"] = "auto_panel_source_v2"
    scope: AutoPanelScope
    kpis: AutoPanelKpis
    top_corridors: Annotated[list[AutoPanelCorridorMetric], _truncate(5)] = Field(default_factory=list, max_length=5)
    top_ports: Annotated[list[AutoPanelPortMetric], _truncate(5)] = Field(default_factory=list, max_length=5)
    deltas: Annotated[list[AutoPanelDelta], _truncate(6)] = Field(default_factory=list, max_length=6)
    signals: Annotated[list[AutoPanelSignal], _truncate(8)] = Field(default_factory=list, max_length=8)
    action_candidates: Annotated[list[AutoPanelActionCandidate], _truncate(4)] = Field(default_factory=list, max_length=4)
    previous_top_corridor_label: str | None = Field(default=None, max_length=160)
    notable_change_reasons: Annotated[list[str], _truncate(4)] = Field(default_factory=list, max_length=4)


class AutoPanelNarrativeOverlay(BaseModel):
    """LLM-generated narrative overlay grounded in the source snapshot."""
    headline: str = Field(min_length=3, max_length=120)
    analyst_note: str = Field(min_length=60, max_length=1200)
    cited_signals: list[str] = Field(default_factory=list, max_length=4)

    @field_validator("cited_signals")
    @classmethod
    def _clean_cited_signals(cls, value: list[str]) -> list[str]:
        """Trim cited signal values and cap the list length."""
        cleaned = [str(v).strip() for v in value if str(v).strip()]
        return cleaned[:4]


class AutoPanelOutput(BaseModel):
    """Rendered panel output persisted for dashboard presentation."""
    title: str = Field(min_length=3, max_length=120)
    summary: str = Field(min_length=10, max_length=600)
    facts: list[str] = Field(default_factory=list, max_length=8)
    anomalies: list[str] = Field(default_factory=list, max_length=8)
    comparisons: list[str] = Field(default_factory=list, max_length=8)
    recommendations: list[str] = Field(default_factory=list, max_length=8)
    report_text: str = Field(min_length=10, max_length=2500)
    insight_text: str | None = None

    @field_validator("facts", "anomalies", "comparisons", "recommendations")
    @classmethod
    def _clean_list(cls, value: list[str]) -> list[str]:
        """Trim list items and enforce the persisted list length cap."""
        cleaned = [str(v).strip() for v in value if str(v).strip()]
        return cleaned[:8]


class GenerationMeta(BaseModel):
    """Metadata describing how a snapshot was generated."""
    generation_mode: str
    generation_error: str | None = None
    model_name: str | None = None


class SnapshotPayload(BaseModel):
    """Complete snapshot record ready to be written to storage."""
    chart_id: str
    insight_type: str
    grain: str
    filter_hash: str | None = None
    data_version: str
    snapshot_ts_iso: str
    output: AutoPanelOutput
    meta: GenerationMeta
    source_snapshot_json: str | None = None
    overlay_json: str | None = None
