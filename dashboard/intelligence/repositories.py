"""Helpers for reading and writing intelligence snapshots to BigQuery."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Callable

import pandas as pd
from google.cloud import bigquery

from .models.schemas import SnapshotPayload
from .models.state import AgentInsightSnapshot, SnapshotFreshness


def build_filter_hash(filters: dict[str, object]) -> str:
    """Hash the active dashboard filters into a short stable string."""
    payload = json.dumps(filters, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _build_top_routes_digest(routes_df: pd.DataFrame, top_n: int = 5) -> str:
    """Build a short digest string from the top passenger corridors for change detection."""
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
    """Build a compact string that describes the current data state — used to detect stale snapshots."""
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


def _normalize_lines(row: pd.Series, *, skip_report_text: bool = False) -> list[str]:
    """Collect the text fields from a snapshot row into a deduplicated list of lines."""
    report_text_value = row.get("report_text")
    if isinstance(report_text_value, str):
        report_text = report_text_value.strip()
        lower = report_text.lower()
        if (
            "current state:" in lower
            and "notable shifts:" in lower
            and "anomaly call-out:" in lower
            and "outlook:" in lower
        ):
            blocks = [part.strip() for part in report_text.split("\n\n") if part.strip()]
            if blocks:
                return blocks

    lines: list[str] = []

    text_cols = ["summary", "insight_text"] if skip_report_text else ["summary", "report_text", "insight_text"]
    for col in text_cols:
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


def _as_string_list(value: object) -> list[str]:
    """Convert a mixed value (list, array, or string) into a list of non-empty strings."""
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if hasattr(value, "tolist"):
        try:
            converted = value.tolist()
        except Exception:
            converted = None
        if isinstance(converted, list):
            return [str(item).strip() for item in converted if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _extract_scope_label(source_snapshot_json: object) -> str | None:
    """Read the scope label out of a stored snapshot JSON string."""
    if not isinstance(source_snapshot_json, str) or not source_snapshot_json.strip():
        return None

    try:
        payload = json.loads(source_snapshot_json)
    except json.JSONDecodeError:
        return None

    scope = payload.get("scope")
    if not isinstance(scope, dict):
        return None

    scope_label = scope.get("scope_label")
    if isinstance(scope_label, str) and scope_label.strip():
        return scope_label.strip()
    return None


def _extract_scope_dates(source_snapshot_json: object) -> tuple[str | None, str | None]:
    """Read the KPI start and end dates out of a stored snapshot JSON string."""
    if not isinstance(source_snapshot_json, str) or not source_snapshot_json.strip():
        return None, None

    try:
        payload = json.loads(source_snapshot_json)
    except json.JSONDecodeError:
        return None, None

    scope = payload.get("scope")
    if not isinstance(scope, dict):
        return None, None

    kpi_start = scope.get("kpi_start")
    kpi_end = scope.get("kpi_end")
    kpi_start_val = str(kpi_start).strip() if isinstance(kpi_start, str) and kpi_start.strip() else None
    kpi_end_val = str(kpi_end).strip() if isinstance(kpi_end, str) and kpi_end.strip() else None
    return kpi_start_val, kpi_end_val


def _build_panel_lines(row: pd.Series) -> list[str]:
    """Build the ordered list of text paragraphs shown in the main agent panel."""
    generation_mode = str(row.get("generation_mode") or "").strip()
    summary = str(row.get("summary") or "").strip()
    report_text = str(row.get("report_text") or "").strip()
    recommendations = _as_string_list(row.get("recommendations"))

    lines: list[str] = []
    include_summary = generation_mode != "llm_overlay"
    if include_summary and summary:
        lines.append(summary)
    if report_text and report_text != summary:
        if generation_mode == "llm_overlay" and "\n\n" in report_text:
            blocks = [block.strip() for block in report_text.split("\n\n") if block.strip()]
            lines.extend(blocks)
        else:
            lines.append(report_text)

    if recommendations and generation_mode != "llm_overlay":
        first_recommendation = recommendations[0]
        if not first_recommendation.lower().startswith("action:"):
            first_recommendation = f"Action: {first_recommendation}"
        lines.append(first_recommendation)
        if len(recommendations) > 1:
            lines.append(recommendations[1])

    deduped: list[str] = []
    for line in lines:
        if line and line not in deduped:
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
    """Fetch the best matching snapshot for a chart and filter combination from BigQuery."""
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
        recommendations,
        generation_mode,
        generation_error,
        model_name,
        source_snapshot_json,
        overlay_json
    FROM {qualify(table_name)}
    WHERE chart_id = @chart_id
      AND data_version = @data_version
      AND (filter_hash = @filter_hash OR filter_hash IS NULL)
      AND (@insight_type IS NULL OR insight_type = @insight_type)
    ORDER BY
        CASE
            WHEN filter_hash = @filter_hash THEN 0
            WHEN filter_hash IS NULL THEN 1
            ELSE 2
        END,
        snapshot_ts DESC
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
    if chart_id == "agent_panel" and insight_type == "auto_panel":
        lines = _build_panel_lines(row)
    else:
        lines = _normalize_lines(row, skip_report_text=(insight_type == "deepen_chart"))
    if not lines:
        return None

    title = str(row.get("title") or "Harbor Intelligence")
    snapshot_ts = row.get("snapshot_ts")
    generated_at = None
    if snapshot_ts is not None and not pd.isna(snapshot_ts):
        generated_at = pd.to_datetime(snapshot_ts).strftime("%d %b %Y %H:%M")

    kpi_start, kpi_end = _extract_scope_dates(row.get("source_snapshot_json"))
    return AgentInsightSnapshot(
        title=title,
        lines=lines,
        generated_at=generated_at,
        generation_mode=str(row.get("generation_mode") or "").strip() or None,
        model_name=str(row.get("model_name") or "").strip() or None,
        scope_label=_extract_scope_label(row.get("source_snapshot_json")),
        kpi_start=kpi_start,
        kpi_end=kpi_end,
    )


def load_latest_agent_snapshot(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    chart_id: str = "agent_panel",
    insight_type: str | None = None,
) -> AgentInsightSnapshot | None:
    """Fetch the most recent snapshot for a chart, ignoring data version."""
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
        recommendations,
        generation_mode,
        generation_error,
        model_name,
        source_snapshot_json,
        overlay_json
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
    if chart_id == "agent_panel" and insight_type == "auto_panel":
        lines = _build_panel_lines(row)
    else:
        lines = _normalize_lines(row, skip_report_text=(insight_type == "deepen_chart"))
    if not lines:
        return None

    title = str(row.get("title") or "Harbor Intelligence")
    snapshot_ts = row.get("snapshot_ts")
    generated_at = None
    if snapshot_ts is not None and not pd.isna(snapshot_ts):
        generated_at = pd.to_datetime(snapshot_ts).strftime("%d %b %Y %H:%M")

    kpi_start, kpi_end = _extract_scope_dates(row.get("source_snapshot_json"))
    return AgentInsightSnapshot(
        title=title,
        lines=lines,
        generated_at=generated_at,
        generation_mode=str(row.get("generation_mode") or "").strip() or None,
        model_name=str(row.get("model_name") or "").strip() or None,
        scope_label=_extract_scope_label(row.get("source_snapshot_json")),
        kpi_start=kpi_start,
        kpi_end=kpi_end,
    )


def load_snapshot_freshness(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    chart_id: str = "agent_panel",
    insight_type: str = "auto_panel",
    stale_after_hours: int = 48,
) -> SnapshotFreshness:
    """Check how old the latest snapshot is and whether it has gone stale."""
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


def upsert_snapshot(
    client: bigquery.Client,
    *,
    table_fqn: str,
    payload: SnapshotPayload,
) -> None:
    """Write a snapshot row to BigQuery, updating it if one already exists for the same chart and version."""
    query = f"""
    MERGE `{table_fqn}` AS target
    USING (
      SELECT
        @snapshot_ts AS snapshot_ts,
        @chart_id AS chart_id,
        @insight_type AS insight_type,
        @grain AS grain,
        @filter_hash AS filter_hash,
        @data_version AS data_version,
        @title AS title,
        @summary AS summary,
        @facts AS facts,
        @anomalies AS anomalies,
        @comparisons AS comparisons,
        @recommendations AS recommendations,
        @report_text AS report_text,
        @insight_text AS insight_text,
        @generation_mode AS generation_mode,
        @generation_error AS generation_error,
        @model_name AS model_name,
        @source_snapshot_json AS source_snapshot_json,
        @overlay_json AS overlay_json
    ) AS src
    ON target.chart_id = src.chart_id
       AND target.insight_type = src.insight_type
       AND target.data_version = src.data_version
       AND IFNULL(target.filter_hash, '') = IFNULL(src.filter_hash, '')
    WHEN MATCHED THEN UPDATE SET
       snapshot_ts = src.snapshot_ts,
       grain = src.grain,
       title = src.title,
       summary = src.summary,
       facts = src.facts,
       anomalies = src.anomalies,
       comparisons = src.comparisons,
       recommendations = src.recommendations,
       report_text = src.report_text,
       insight_text = src.insight_text,
       generation_mode = src.generation_mode,
       generation_error = src.generation_error,
       model_name = src.model_name,
       source_snapshot_json = src.source_snapshot_json,
       overlay_json = src.overlay_json
    WHEN NOT MATCHED THEN
      INSERT (
        snapshot_ts,
        chart_id,
        insight_type,
        grain,
        filter_hash,
        data_version,
        title,
        summary,
        facts,
        anomalies,
        comparisons,
        recommendations,
        report_text,
        insight_text,
        generation_mode,
        generation_error,
        model_name,
        source_snapshot_json,
        overlay_json
      )
      VALUES (
        src.snapshot_ts,
        src.chart_id,
        src.insight_type,
        src.grain,
        src.filter_hash,
        src.data_version,
        src.title,
        src.summary,
        src.facts,
        src.anomalies,
        src.comparisons,
        src.recommendations,
        src.report_text,
        src.insight_text,
        src.generation_mode,
        src.generation_error,
        src.model_name,
        src.source_snapshot_json,
        src.overlay_json
      )
    """

    params = [
        bigquery.ScalarQueryParameter("snapshot_ts", "TIMESTAMP", payload.snapshot_ts_iso),
        bigquery.ScalarQueryParameter("chart_id", "STRING", payload.chart_id),
        bigquery.ScalarQueryParameter("insight_type", "STRING", payload.insight_type),
        bigquery.ScalarQueryParameter("grain", "STRING", payload.grain),
        bigquery.ScalarQueryParameter("filter_hash", "STRING", payload.filter_hash),
        bigquery.ScalarQueryParameter("data_version", "STRING", payload.data_version),
        bigquery.ScalarQueryParameter("title", "STRING", payload.output.title),
        bigquery.ScalarQueryParameter("summary", "STRING", payload.output.summary),
        bigquery.ArrayQueryParameter("facts", "STRING", payload.output.facts),
        bigquery.ArrayQueryParameter("anomalies", "STRING", payload.output.anomalies),
        bigquery.ArrayQueryParameter("comparisons", "STRING", payload.output.comparisons),
        bigquery.ArrayQueryParameter("recommendations", "STRING", payload.output.recommendations),
        bigquery.ScalarQueryParameter("report_text", "STRING", payload.output.report_text),
        bigquery.ScalarQueryParameter("insight_text", "STRING", payload.output.insight_text),
        bigquery.ScalarQueryParameter("generation_mode", "STRING", payload.meta.generation_mode),
        bigquery.ScalarQueryParameter("generation_error", "STRING", payload.meta.generation_error),
        bigquery.ScalarQueryParameter("model_name", "STRING", payload.meta.model_name),
        bigquery.ScalarQueryParameter("source_snapshot_json", "STRING", payload.source_snapshot_json),
        bigquery.ScalarQueryParameter("overlay_json", "STRING", payload.overlay_json),
    ]

    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()
