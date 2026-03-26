from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable

import pandas as pd


@dataclass
class AgentInsightSnapshot:
    title: str
    lines: list[str]
    generated_at: str | None = None


@dataclass
class SnapshotFreshness:
    latest_snapshot_ts: datetime | None
    hours_since_latest: float | None
    is_stale: bool


def build_filter_hash(filters: dict[str, object]) -> str:
    payload = json.dumps(filters, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _build_top_routes_digest(routes_df: pd.DataFrame, top_n: int = 5) -> str:
    if routes_df.empty:
        return "none"

    required = {"departure_port", "arrival_port", "total_passengers"}
    if not required.issubset(routes_df.columns):
        return "none"

    scoped = routes_df[["departure_port", "arrival_port", "total_passengers"]].copy()
    scoped["departure_port"] = scoped["departure_port"].fillna("Unknown").astype(str)
    scoped["arrival_port"] = scoped["arrival_port"].fillna("Unknown").astype(str)
    scoped["total_passengers"] = pd.to_numeric(scoped["total_passengers"], errors="coerce").fillna(0.0)

    grouped = (
        scoped.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"]
        .sum()
        .sort_values(["total_passengers", "departure_port", "arrival_port"], ascending=[False, True, True])
        .head(top_n)
    )

    if grouped.empty:
        return "none"

    parts = [
        f"{row['departure_port']}->{row['arrival_port']}:{int(row['total_passengers'])}"
        for _, row in grouped.iterrows()
    ]
    return "|".join(parts) if parts else "none"


def build_data_version(daily_df: pd.DataFrame, routes_df: pd.DataFrame | None = None) -> str:
    if daily_df.empty:
        return "empty"

    max_date = pd.to_datetime(daily_df["service_date"], errors="coerce").max()
    max_date_text = max_date.date().isoformat() if pd.notna(max_date) else "unknown"

    rows_total = 0
    if "rows_count" in daily_df.columns:
        rows_total = int(pd.to_numeric(daily_df["rows_count"], errors="coerce").fillna(0).sum())

    passengers_total = 0
    if "total_passengers" in daily_df.columns:
        passengers_total = int(pd.to_numeric(daily_df["total_passengers"], errors="coerce").fillna(0).sum())

    top_routes_digest = _build_top_routes_digest(routes_df if routes_df is not None else pd.DataFrame())
    top_routes_hash = hashlib.sha256(top_routes_digest.encode("utf-8")).hexdigest()[:16]

    return f"{max_date_text}|rows:{rows_total}|pax:{passengers_total}|top:{top_routes_hash}"


def _normalize_lines(row: pd.Series) -> list[str]:
    lines: list[str] = []

    for col in ["summary", "report_text", "insight_text"]:
        value = row.get(col)
        if isinstance(value, str) and value.strip():
            lines.append(value.strip())

    for col in ["facts", "anomalies", "comparisons", "recommendations"]:
        value = row.get(col)
        if isinstance(value, list):
            lines.extend([str(item).strip() for item in value if str(item).strip()])
        elif isinstance(value, str) and value.strip():
            lines.append(value.strip())

    deduped: list[str] = []
    for line in lines:
        if line not in deduped:
            deduped.append(line)
    return deduped


def load_agent_snapshot(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    data_version: str,
    filter_hash: str,
    chart_id: str = "agent_panel",
    insight_type: str | None = None,
) -> AgentInsightSnapshot | None:
    query = f"""
    SELECT
        snapshot_ts,
        title,
        summary,
        report_text,
        insight_text,
        facts,
        anomalies,
        comparisons,
        recommendations
    FROM {qualify(table_name)}
    WHERE chart_id = @chart_id
      AND data_version = @data_version
      AND (filter_hash = @filter_hash OR filter_hash IS NULL)
      AND (@insight_type IS NULL OR insight_type = @insight_type)
    ORDER BY snapshot_ts DESC
    LIMIT 1
    """

    try:
        df = run_query(
            query,
            scalar_params=(
                ("chart_id", "STRING", chart_id),
                ("data_version", "STRING", data_version),
                ("filter_hash", "STRING", filter_hash),
                ("insight_type", "STRING", insight_type),
            ),
        )
    except Exception:
        return None

    if df.empty:
        return None

    row = df.iloc[0]
    lines = _normalize_lines(row)
    if not lines:
        return None

    title = str(row.get("title") or "Harbor Intelligence")
    snapshot_ts = row.get("snapshot_ts")
    generated_at = None
    if snapshot_ts is not None and not pd.isna(snapshot_ts):
        generated_at = pd.to_datetime(snapshot_ts).strftime("%d %b %Y %H:%M")

    return AgentInsightSnapshot(title=title, lines=lines, generated_at=generated_at)


def load_latest_agent_snapshot(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    chart_id: str = "agent_panel",
    insight_type: str | None = None,
) -> AgentInsightSnapshot | None:
    query = f"""
    SELECT
        snapshot_ts,
        title,
        summary,
        report_text,
        insight_text,
        facts,
        anomalies,
        comparisons,
        recommendations
    FROM {qualify(table_name)}
    WHERE chart_id = @chart_id
      AND (@insight_type IS NULL OR insight_type = @insight_type)
    ORDER BY snapshot_ts DESC
    LIMIT 1
    """

    try:
        df = run_query(
            query,
            scalar_params=(
                ("chart_id", "STRING", chart_id),
                ("insight_type", "STRING", insight_type),
            ),
        )
    except Exception:
        return None

    if df.empty:
        return None

    row = df.iloc[0]
    lines = _normalize_lines(row)
    if not lines:
        return None

    title = str(row.get("title") or "Harbor Intelligence")
    snapshot_ts = row.get("snapshot_ts")
    generated_at = None
    if snapshot_ts is not None and not pd.isna(snapshot_ts):
        generated_at = pd.to_datetime(snapshot_ts).strftime("%d %b %Y %H:%M")

    return AgentInsightSnapshot(title=title, lines=lines, generated_at=generated_at)


def load_snapshot_freshness(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    chart_id: str = "agent_panel",
    insight_type: str = "auto_panel",
    stale_after_hours: int = 48,
) -> SnapshotFreshness:
    query = f"""
    SELECT MAX(snapshot_ts) AS latest_snapshot_ts
    FROM {qualify(table_name)}
    WHERE chart_id = @chart_id
      AND insight_type = @insight_type
    """

    try:
        df = run_query(
            query,
            scalar_params=(
                ("chart_id", "STRING", chart_id),
                ("insight_type", "STRING", insight_type),
            ),
        )
    except Exception:
        return SnapshotFreshness(latest_snapshot_ts=None, hours_since_latest=None, is_stale=True)

    if df.empty or pd.isna(df.iloc[0].get("latest_snapshot_ts")):
        return SnapshotFreshness(latest_snapshot_ts=None, hours_since_latest=None, is_stale=True)

    latest_ts = pd.to_datetime(df.iloc[0]["latest_snapshot_ts"], utc=True).to_pydatetime()
    now_ts = datetime.now(UTC)
    hours_since = (now_ts - latest_ts).total_seconds() / 3600

    return SnapshotFreshness(
        latest_snapshot_ts=latest_ts,
        hours_since_latest=hours_since,
        is_stale=hours_since > stale_after_hours,
    )
