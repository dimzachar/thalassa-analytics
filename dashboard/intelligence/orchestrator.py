"""Builds the dashboard intelligence state from cached snapshots and live data."""

from __future__ import annotations

from typing import Callable

import pandas as pd

from .agents.detector_agent import detect_notable_change
from .models.state import AgentContext
from .repositories import (
    build_data_version,
    build_filter_hash,
    load_agent_snapshot,
    load_latest_agent_snapshot,
    load_snapshot_freshness,
)


def build_agent_context(
    run_query: Callable[..., pd.DataFrame],
    qualify: Callable[[str], str],
    *,
    table_name: str,
    daily_df: pd.DataFrame,
    routes_df: pd.DataFrame,
    filters: dict[str, object],
    stale_after_hours: int = 48,
) -> AgentContext:
    """Load snapshots, check freshness, and detect notable changes for the UI panels."""
    filter_hash = build_filter_hash(filters)
    data_version = build_data_version(daily_df, routes_df)

    panel_snapshot = load_agent_snapshot(
        run_query,
        qualify,
        table_name=table_name,
        data_version=data_version,
        filter_hash=filter_hash,
        chart_id="agent_panel",
        insight_type="auto_panel",
    )
    if panel_snapshot is None:
        panel_snapshot = load_latest_agent_snapshot(
            run_query,
            qualify,
            table_name=table_name,
            chart_id="agent_panel",
            insight_type="auto_panel",
        )

    freshness = load_snapshot_freshness(
        run_query,
        qualify,
        table_name=table_name,
        chart_id="agent_panel",
        insight_type="auto_panel",
        stale_after_hours=stale_after_hours,
    )

    notable_change = detect_notable_change(daily_df, routes_df)

    # TODO: deepen_chart feature not yet implemented (planned: LLM anomaly analysis per chart)
    # deepen_traffic_pulse_snapshot = load_agent_snapshot(...)
    # deepen_top_corridors_snapshot = load_agent_snapshot(...)
    # deepen_port_balance_snapshot = load_agent_snapshot(...)

    return AgentContext(
        filter_hash=filter_hash,
        data_version=data_version,
        panel_snapshot=panel_snapshot,
        freshness=freshness,
        notable_change=notable_change,
        deepen_traffic_pulse_snapshot=None,
        deepen_top_corridors_snapshot=None,
        deepen_port_balance_snapshot=None,
    )
