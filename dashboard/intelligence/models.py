from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class AutoPanelOutput(BaseModel):
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
        cleaned = [str(v).strip() for v in value if str(v).strip()]
        return cleaned[:8]


class GenerationMeta(BaseModel):
    generation_mode: str
    generation_error: str | None = None
    model_name: str | None = None


class SnapshotPayload(BaseModel):
    chart_id: str
    insight_type: str
    grain: str
    filter_hash: str | None = None
    data_version: str
    snapshot_ts_iso: str
    output: AutoPanelOutput
    meta: GenerationMeta
