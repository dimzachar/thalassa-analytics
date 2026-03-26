from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from .detector import NotableChangeDecision, detect_notable_change
from .panel import (
    AgentInsightSnapshot,
    SnapshotFreshness,
    build_data_version,
    build_filter_hash,
    load_agent_snapshot,
    load_latest_agent_snapshot,
    load_snapshot_freshness,
)


@dataclass
class AgentContext:
    filter_hash: str
    data_version: str
    panel_snapshot: AgentInsightSnapshot | None
    freshness: SnapshotFreshness
    notable_change: NotableChangeDecision
    deepen_traffic_pulse_snapshot: AgentInsightSnapshot | None
    deepen_top_corridors_snapshot: AgentInsightSnapshot | None
    deepen_port_balance_snapshot: AgentInsightSnapshot | None


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
    filter_hash = build_filter_hash(filters)
    data_version = build_data_version(daily_df, routes_df)

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

    deepen_traffic_pulse_snapshot = load_agent_snapshot(
        run_query,
        qualify,
        table_name=table_name,
        data_version=data_version,
        filter_hash=filter_hash,
        chart_id="traffic_pulse",
        insight_type="deepen_chart",
    )
    deepen_top_corridors_snapshot = load_agent_snapshot(
        run_query,
        qualify,
        table_name=table_name,
        data_version=data_version,
        filter_hash=filter_hash,
        chart_id="top_corridors",
        insight_type="deepen_chart",
    )
    deepen_port_balance_snapshot = load_agent_snapshot(
        run_query,
        qualify,
        table_name=table_name,
        data_version=data_version,
        filter_hash=filter_hash,
        chart_id="port_balance",
        insight_type="deepen_chart",
    )

    return AgentContext(
        filter_hash=filter_hash,
        data_version=data_version,
        panel_snapshot=panel_snapshot,
        freshness=freshness,
        notable_change=notable_change,
        deepen_traffic_pulse_snapshot=deepen_traffic_pulse_snapshot,
        deepen_top_corridors_snapshot=deepen_top_corridors_snapshot,
        deepen_port_balance_snapshot=deepen_port_balance_snapshot,
    )
