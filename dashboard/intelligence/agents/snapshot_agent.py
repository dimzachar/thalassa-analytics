"""Snapshot builders for turning traffic metrics into structured intelligence inputs."""

from __future__ import annotations

import math

import pandas as pd

from ..models.schemas import (
    AutoPanelActionCandidate,
    AutoPanelCorridorMetric,
    AutoPanelDelta,
    AutoPanelKpis,
    AutoPanelNarrativeOverlay,
    AutoPanelOutput,
    AutoPanelPortMetric,
    AutoPanelScope,
    AutoPanelSignal,
    AutoPanelSourceSnapshot,
)
from ..models.state import NotableChangeDecision


def _fmt_compact(value: float | int) -> str:
    """Format large numeric values into compact human-readable strings."""
    numeric = float(value)
    absolute = abs(numeric)
    if absolute >= 1_000_000:
        return f"{numeric / 1_000_000:.2f}M"
    if absolute >= 100_000:
        return f"{numeric / 1_000:.0f}k"
    if absolute >= 10_000:
        return f"{numeric / 1_000:.1f}k"
    return f"{numeric:,.0f}"


def _direction(delta_pct: float | None, *, tolerance: float = 1.0) -> str:
    """Convert a percentage delta into a coarse direction label."""
    if delta_pct is None or not math.isfinite(delta_pct):
        return "unknown"
    if delta_pct > tolerance:
        return "up"
    if delta_pct < -tolerance:
        return "down"
    return "flat"


def _severity(delta_pct: float | None, *, watch_pct: float = 10.0, alert_pct: float = 20.0) -> str:
    """Map a percentage delta to an informational, watch, or alert severity."""
    if delta_pct is None or not math.isfinite(delta_pct):
        return "info"
    absolute = abs(delta_pct)
    if absolute >= alert_pct:
        return "alert"
    if absolute >= watch_pct:
        return "watch"
    return "info"


def _prepare_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """Normalize and sort daily metrics before snapshot calculations."""
    if daily.empty:
        return daily.copy()

    prepared = daily.copy()
    prepared["service_date"] = pd.to_datetime(prepared["service_date"], errors="coerce")
    prepared = prepared.dropna(subset=["service_date"]).sort_values("service_date")
    for column in ["rows_count", "total_passengers", "total_vehicles"]:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce").fillna(0.0)
        else:
            prepared[column] = 0.0
    return prepared


def _prepare_routes(routes: pd.DataFrame) -> pd.DataFrame:
    """Normalize and sort route-level traffic data for corridor aggregations."""
    if routes.empty:
        return routes.copy()

    prepared = routes.copy()
    prepared["service_date"] = pd.to_datetime(prepared["service_date"], errors="coerce")
    prepared = prepared.dropna(subset=["service_date"]).sort_values("service_date")
    prepared["departure_port"] = prepared.get("departure_port", pd.Series(dtype=str)).fillna("Unknown").astype(str)
    prepared["arrival_port"] = prepared.get("arrival_port", pd.Series(dtype=str)).fillna("Unknown").astype(str)
    prepared["total_passengers"] = pd.to_numeric(
        prepared.get("total_passengers", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    prepared["total_vehicles"] = pd.to_numeric(
        prepared.get("total_vehicles", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    return prepared


def _prepare_ports(ports: pd.DataFrame) -> pd.DataFrame:
    """Normalize and enrich port activity data with total passenger volume."""
    if ports.empty:
        return ports.copy()

    prepared = ports.copy()
    prepared["service_date"] = pd.to_datetime(prepared["service_date"], errors="coerce")
    prepared = prepared.dropna(subset=["service_date"]).sort_values("service_date")
    prepared["port_name"] = prepared.get("port_name", pd.Series(dtype=str)).fillna("Unknown").astype(str)
    departing = pd.to_numeric(
        prepared.get("total_departing_passengers", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    arriving = pd.to_numeric(
        prepared.get("total_arriving_passengers", pd.Series(dtype=float)),
        errors="coerce",
    ).fillna(0.0)
    prepared["total_passengers"] = departing + arriving
    return prepared


def _build_delta(
    *,
    name: str,
    label: str,
    period: str,
    current_value: float | None,
    previous_value: float | None,
) -> AutoPanelDelta:
    """Build a named delta record comparing current and prior window values."""
    delta_pct = None
    if (
        current_value is not None
        and previous_value is not None
        and math.isfinite(current_value)
        and math.isfinite(previous_value)
        and previous_value > 0
    ):
        delta_pct = ((current_value - previous_value) / previous_value) * 100.0

    if delta_pct is None:
        summary = f"{label}: insufficient history for a {period} comparison."
    else:
        summary = f"{label}: {delta_pct:+.1f}% versus the prior {period}."

    return AutoPanelDelta(
        name=name,
        label=label,
        period=period,
        direction=_direction(delta_pct),
        severity=_severity(delta_pct),
        value_pct=delta_pct,
        summary=summary,
    )


def _build_top_corridors(routes_kpi: pd.DataFrame, passengers_total: float) -> list[AutoPanelCorridorMetric]:
    """Build the ranked top-corridor metrics for the current KPI window."""
    if routes_kpi.empty:
        return []

    grouped = (
        routes_kpi.groupby(["departure_port", "arrival_port"], as_index=False)[["total_passengers", "total_vehicles"]]
        .sum()
        .sort_values(["total_passengers", "total_vehicles", "departure_port", "arrival_port"], ascending=[False, False, True, True])
        .head(5)
    )
    grouped["pair_label"] = grouped["departure_port"] + " -> " + grouped["arrival_port"]

    results: list[AutoPanelCorridorMetric] = []
    for _, row in grouped.iterrows():
        share_pct = (float(row["total_passengers"]) / passengers_total) * 100.0 if passengers_total > 0 else 0.0
        results.append(
            AutoPanelCorridorMetric(
                pair_label=str(row["pair_label"]),
                passengers=int(round(float(row["total_passengers"]))),
                vehicles=int(round(float(row["total_vehicles"]))),
                share_pct=round(share_pct, 1),
            )
        )
    return results


def _build_top_ports(ports_kpi: pd.DataFrame) -> list[AutoPanelPortMetric]:
    """Build the ranked top-port metrics for the current KPI window."""
    if ports_kpi.empty:
        return []

    grouped = (
        ports_kpi.groupby("port_name", as_index=False)["total_passengers"]
        .sum()
        .sort_values(["total_passengers", "port_name"], ascending=[False, True])
        .head(5)
    )

    return [
        AutoPanelPortMetric(
            port_name=str(row["port_name"]),
            passengers=int(round(float(row["total_passengers"]))),
        )
        for _, row in grouped.iterrows()
    ]


def _find_previous_top_corridor(routes_scope: pd.DataFrame, end_ts: pd.Timestamp) -> str | None:
    """Find the leading corridor in the prior weekly comparison window."""
    if routes_scope.empty:
        return None

    prior_start = end_ts - pd.Timedelta(days=13)
    prior_end = end_ts - pd.Timedelta(days=7)
    prior = routes_scope[(routes_scope["service_date"] >= prior_start) & (routes_scope["service_date"] <= prior_end)].copy()
    if prior.empty:
        return None

    grouped = (
        prior.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"]
        .sum()
        .sort_values(["total_passengers", "departure_port", "arrival_port"], ascending=[False, True, True])
    )
    if grouped.empty:
        return None

    row = grouped.iloc[0]
    return f"{row['departure_port']} -> {row['arrival_port']}"


def _build_signals(
    *,
    deltas: list[AutoPanelDelta],
    top_corridors: list[AutoPanelCorridorMetric],
    previous_top_corridor_label: str | None,
    congestion_index_pct: float,
) -> list[AutoPanelSignal]:
    """Assemble deduplicated signal records from deltas and concentration metrics."""
    signals: list[AutoPanelSignal] = [
        AutoPanelSignal(kind=delta.name, severity=delta.severity, summary=delta.summary)
        for delta in deltas
    ]

    if top_corridors:
        top_corridor = top_corridors[0]
        concentration_severity = "alert" if top_corridor.share_pct >= 45 else "watch" if top_corridor.share_pct >= 30 else "info"
        signals.append(
            AutoPanelSignal(
                kind="corridor_concentration",
                severity=concentration_severity,
                summary=(
                    f"Top corridor concentration: {top_corridor.pair_label} carries {top_corridor.share_pct:.1f}% "
                    "of current-period passengers."
                ),
            )
        )
        if previous_top_corridor_label:
            if previous_top_corridor_label == top_corridor.pair_label:
                summary = f"Corridor leader is stable: {top_corridor.pair_label} remains first across the last two weekly windows."
                severity = "info"
            else:
                summary = (
                    f"Corridor rotation detected: leader moved from {previous_top_corridor_label} "
                    f"to {top_corridor.pair_label} in the latest weekly window."
                )
                severity = "watch"
            signals.append(AutoPanelSignal(kind="corridor_shift", severity=severity, summary=summary))

    congestion_severity = "alert" if congestion_index_pct >= 90 else "watch" if congestion_index_pct >= 75 else "info"
    signals.append(
        AutoPanelSignal(
            kind="congestion",
            severity=congestion_severity,
            summary=f"Congestion index is {congestion_index_pct:.0f}% versus the 95th percentile traffic-row baseline.",
        )
    )

    deduped: list[AutoPanelSignal] = []
    seen: set[str] = set()
    for signal in signals:
        if signal.summary in seen:
            continue
        seen.add(signal.summary)
        deduped.append(signal)
    return deduped[:8]


def _build_actions(
    *,
    top_corridors: list[AutoPanelCorridorMetric],
    top_ports: list[AutoPanelPortMetric],
    primary_delta: AutoPanelDelta | None,
    previous_top_corridor_label: str | None,
) -> list[AutoPanelActionCandidate]:
    """Suggest follow-up operational actions from the strongest current signals."""
    actions: list[AutoPanelActionCandidate] = []
    top_corridor = top_corridors[0] if top_corridors else None
    top_port = top_ports[0] if top_ports else None

    if primary_delta is not None and primary_delta.value_pct is not None and primary_delta.value_pct <= -20:
        focus = top_corridor.pair_label if top_corridor else "the leading corridors"
        actions.append(
            AutoPanelActionCandidate(
                action=f"Investigate the demand drop on {focus}.",
                rationale="A large weekly contraction can reflect schedule suppression, displaced demand, or data latency that needs confirmation before action.",
                priority="investigate",
            )
        )
    elif primary_delta is not None and primary_delta.value_pct is not None and primary_delta.value_pct >= 20:
        focus = top_port.port_name if top_port else "the busiest ports"
        actions.append(
            AutoPanelActionCandidate(
                action=f"Validate berth and vessel capacity around {focus}.",
                rationale="A sharp weekly expansion can create staffing and berth pressure if it persists into the next reporting window.",
                priority="investigate",
            )
        )

    if top_corridor and top_corridor.share_pct >= 45:
        actions.append(
            AutoPanelActionCandidate(
                action=f"Review concentration risk on {top_corridor.pair_label}.",
                rationale="High corridor concentration increases operational sensitivity to one route's schedule changes or reporting gaps.",
                priority="escalate",
            )
        )

    if top_corridor and previous_top_corridor_label and previous_top_corridor_label != top_corridor.pair_label:
        actions.append(
            AutoPanelActionCandidate(
                action=f"Confirm the corridor rotation into {top_corridor.pair_label}.",
                rationale="A new lead corridor should be checked against schedule changes and recent demand redistribution before it is treated as a durable shift.",
                priority="monitor",
            )
        )

    if not actions:
        focus = top_port.port_name if top_port else "the network"
        actions.append(
            AutoPanelActionCandidate(
                action=f"Monitor the next ingestion window for {focus}.",
                rationale="The current snapshot does not justify escalation, but the next update should confirm whether recent demand patterns persist or revert.",
                priority="monitor",
            )
        )

    deduped: list[AutoPanelActionCandidate] = []
    seen: set[str] = set()
    for action in actions:
        if action.action in seen:
            continue
        seen.add(action.action)
        deduped.append(action)
    return deduped[:4]


def build_auto_panel_source_snapshot(
    *,
    daily: pd.DataFrame,
    routes: pd.DataFrame,
    ports: pd.DataFrame,
    notable_change: NotableChangeDecision,
    kpi_window_days: int = 7,
) -> AutoPanelSourceSnapshot:
    """Build the structured source snapshot for deterministic and LLM rendering."""
    daily_scope = _prepare_daily(daily)
    routes_scope = _prepare_routes(routes)
    ports_scope = _prepare_ports(ports)

    if daily_scope.empty:
        scope = AutoPanelScope(
            baseline_start="unknown",
            baseline_end="unknown",
            kpi_start="unknown",
            kpi_end="unknown",
            baseline_label="No comparison baseline available",
            kpi_label="No current period available",
            scope_label="No data",
        )
        kpis = AutoPanelKpis(
            passengers=0,
            vehicles=0,
            traffic_rows=0,
            active_corridors=0,
            congestion_index_pct=0.0,
            recent_7d_avg_passengers=0.0,
            prior_7d_avg_passengers=None,
            recent_30d_avg_passengers=None,
            prior_30d_avg_passengers=None,
        )
        return AutoPanelSourceSnapshot(
            scope=scope,
            kpis=kpis,
            top_corridors=[],
            top_ports=[],
            deltas=[],
            signals=[],
            action_candidates=[
                AutoPanelActionCandidate(
                    action="Validate upstream ingestion before publishing commentary.",
                    rationale="The reporting snapshot is empty, so any narrative would be unreliable.",
                    priority="escalate",
                )
            ],
            previous_top_corridor_label=None,
            notable_change_reasons=notable_change.reasons,
        )

    end_ts = daily_scope["service_date"].max()
    view_start_ts = daily_scope["service_date"].min()
    kpi_window_days = max(7, int(kpi_window_days))
    kpi_start_ts = max(view_start_ts, end_ts - pd.Timedelta(days=kpi_window_days - 1))

    baseline_end_ts = kpi_start_ts - pd.Timedelta(days=1)
    baseline_start_ts = view_start_ts
    if baseline_end_ts >= view_start_ts:
        full_weeks_start = baseline_end_ts - pd.Timedelta(days=55)
        if full_weeks_start >= view_start_ts:
            baseline_start_ts = full_weeks_start
        else:
            baseline_start_ts = view_start_ts

    daily_kpi = daily_scope[(daily_scope["service_date"] >= kpi_start_ts) & (daily_scope["service_date"] <= end_ts)].copy()
    routes_kpi = routes_scope[(routes_scope["service_date"] >= kpi_start_ts) & (routes_scope["service_date"] <= end_ts)].copy()
    ports_kpi = ports_scope[(ports_scope["service_date"] >= kpi_start_ts) & (ports_scope["service_date"] <= end_ts)].copy()

    passengers_total = float(daily_kpi["total_passengers"].sum())
    vehicles_total = float(daily_kpi["total_vehicles"].sum())
    traffic_rows_total = float(daily_kpi["rows_count"].sum())
    active_corridors = int(routes_kpi.groupby(["departure_port", "arrival_port"]).ngroups) if not routes_kpi.empty else 0

    recent_rows_avg = float(daily_scope.tail(7)["rows_count"].mean()) if not daily_scope.empty else 0.0
    reference_peak = float(daily_scope["rows_count"].quantile(0.95)) if not daily_scope.empty else 0.0
    congestion_index_pct = min((recent_rows_avg / reference_peak) * 100.0, 100.0) if reference_peak > 0 else 0.0

    recent_7d_avg = float(daily_scope.tail(7)["total_passengers"].mean()) if len(daily_scope) >= 1 else 0.0
    prior_7d_avg = float(daily_scope.iloc[-14:-7]["total_passengers"].mean()) if len(daily_scope) >= 14 else None
    recent_30d_avg = float(daily_scope.tail(30)["total_passengers"].mean()) if len(daily_scope) >= 30 else None
    prior_30d_avg = float(daily_scope.iloc[-60:-30]["total_passengers"].mean()) if len(daily_scope) >= 60 else None

    top_corridors = _build_top_corridors(routes_kpi, passengers_total)
    top_ports = _build_top_ports(ports_kpi)
    previous_top_corridor_label = _find_previous_top_corridor(routes_scope, end_ts)

    coverage_window_avg = float(daily_scope.tail(kpi_window_days)["total_passengers"].mean()) if len(daily_scope) >= 1 else 0.0
    prior_window_avg = (
        float(daily_scope.iloc[-(2 * kpi_window_days) : -kpi_window_days]["total_passengers"].mean())
        if len(daily_scope) >= 2 * kpi_window_days
        else None
    )
    window_delta = _build_delta(
        name="passengers_window_avg",
        label=f"{kpi_window_days}-day average passengers",
        period=f"{kpi_window_days}-day window",
        current_value=coverage_window_avg,
        previous_value=prior_window_avg,
    )
    weekly_delta = _build_delta(
        name="passengers_7d_avg",
        label="7-day average passengers",
        period="7-day window",
        current_value=recent_7d_avg,
        previous_value=prior_7d_avg,
    )
    monthly_delta = _build_delta(
        name="passengers_30d_avg",
        label="30-day average passengers",
        period="30-day window",
        current_value=recent_30d_avg,
        previous_value=prior_30d_avg,
    )
    if kpi_window_days >= 90:
        deltas = [window_delta]
    else:
        deltas = [weekly_delta, monthly_delta]

    signals = _build_signals(
        deltas=deltas,
        top_corridors=top_corridors,
        previous_top_corridor_label=previous_top_corridor_label,
        congestion_index_pct=round(congestion_index_pct, 1),
    )
    primary_delta = deltas[0] if deltas else None
    action_candidates = _build_actions(
        top_corridors=top_corridors,
        top_ports=top_ports,
        primary_delta=primary_delta,
        previous_top_corridor_label=previous_top_corridor_label,
    )

    if baseline_end_ts < view_start_ts:
        baseline_label = "No comparison baseline available"
        baseline_start_str = view_start_ts.date().isoformat()
        baseline_end_str = view_start_ts.date().isoformat()
    else:
        baseline_label = f"{baseline_start_ts.strftime('%d %b %Y')} -> {baseline_end_ts.strftime('%d %b %Y')}"
        baseline_start_str = baseline_start_ts.date().isoformat()
        baseline_end_str = baseline_end_ts.date().isoformat()

    scope = AutoPanelScope(
        baseline_start=baseline_start_str,
        baseline_end=baseline_end_str,
        kpi_start=kpi_start_ts.date().isoformat(),
        kpi_end=end_ts.date().isoformat(),
        baseline_label=baseline_label,
        kpi_label=f"Current period: {kpi_start_ts.strftime('%d %b %Y')} -> {end_ts.strftime('%d %b %Y')}",
        scope_label=f"Global network | Report period {kpi_start_ts.strftime('%d %b %Y')} -> {end_ts.strftime('%d %b %Y')}",
    )
    kpis = AutoPanelKpis(
        passengers=int(round(passengers_total)),
        vehicles=int(round(vehicles_total)),
        traffic_rows=int(round(traffic_rows_total)),
        active_corridors=active_corridors,
        congestion_index_pct=round(congestion_index_pct, 1),
        recent_7d_avg_passengers=round(recent_7d_avg, 1),
        prior_7d_avg_passengers=round(prior_7d_avg, 1) if prior_7d_avg is not None else None,
        recent_30d_avg_passengers=round(recent_30d_avg, 1) if recent_30d_avg is not None else None,
        prior_30d_avg_passengers=round(prior_30d_avg, 1) if prior_30d_avg is not None else None,
    )

    return AutoPanelSourceSnapshot(
        scope=scope,
        kpis=kpis,
        top_corridors=top_corridors,
        top_ports=top_ports,
        deltas=deltas,
        signals=signals,
        action_candidates=action_candidates,
        previous_top_corridor_label=previous_top_corridor_label,
        notable_change_reasons=notable_change.reasons,
    )


def _deterministic_outlook(source_snapshot: AutoPanelSourceSnapshot) -> str:
    """Generate a short deterministic outlook sentence from recent deltas."""
    weekly_delta = next((delta for delta in source_snapshot.deltas if delta.name == "passengers_7d_avg"), None)
    if weekly_delta is None or weekly_delta.value_pct is None:
        return "Hold the current view until another reporting window lands."
    if weekly_delta.value_pct >= 8:
        return "Expect demand to stay firm or edge higher in the next reporting window unless the spike reverses quickly."
    if weekly_delta.value_pct <= -8:
        return "Expect a soft patch in the next reporting window unless this week's drop proves to be temporary."
    return "Expect broadly stable movement in the next reporting window."


def _deterministic_brief(source_snapshot: AutoPanelSourceSnapshot) -> str:
    """Generate the compact deterministic report text for fallback rendering."""
    weekly_delta = next((delta for delta in source_snapshot.deltas if delta.name == "passengers_7d_avg"), None)
    monthly_delta = next((delta for delta in source_snapshot.deltas if delta.name == "passengers_30d_avg"), None)
    top_corridor = source_snapshot.top_corridors[0] if source_snapshot.top_corridors else None

    parts: list[str] = []
    if weekly_delta is not None:
        parts.append(weekly_delta.summary)
    if monthly_delta is not None and monthly_delta.value_pct is not None:
        parts.append(monthly_delta.summary)
    if top_corridor and source_snapshot.previous_top_corridor_label:
        if top_corridor.pair_label == source_snapshot.previous_top_corridor_label:
            parts.append(f"Route leadership remains anchored on {top_corridor.pair_label}.")
        else:
            parts.append(
                f"Route leadership rotated from {source_snapshot.previous_top_corridor_label} to {top_corridor.pair_label}."
            )
    parts.append(_deterministic_outlook(source_snapshot))
    return " ".join(parts)


def render_auto_panel_output(
    *,
    source_snapshot: AutoPanelSourceSnapshot,
    overlay: AutoPanelNarrativeOverlay | None = None,
) -> AutoPanelOutput:
    """Render the final panel output from a source snapshot and optional overlay."""
    top_corridor = source_snapshot.top_corridors[0] if source_snapshot.top_corridors else None
    top_port = source_snapshot.top_ports[0] if source_snapshot.top_ports else None

    summary = (
        f"Current period: {_fmt_compact(source_snapshot.kpis.passengers)} passengers and "
        f"{_fmt_compact(source_snapshot.kpis.vehicles)} vehicles. "
        f"Congestion held at {source_snapshot.kpis.congestion_index_pct:.0f}% of baseline."
    )
    if top_corridor:
        summary += f" {top_corridor.pair_label} led with {top_corridor.share_pct:.1f}% of traffic."
    if top_port:
        summary += f" {top_port.port_name} remained the busiest port."

    facts: list[str] = [
        f"Comparison baseline: {source_snapshot.scope.baseline_label}.",
        f"Traffic rows in the current period: {_fmt_compact(source_snapshot.kpis.traffic_rows)} across {source_snapshot.kpis.active_corridors} active corridors.",
    ]
    if top_corridor:
        facts.append(
            f"Top corridor {top_corridor.pair_label} carried {_fmt_compact(top_corridor.passengers)} passengers ({top_corridor.share_pct:.1f}% share)."
        )
    if top_port:
        facts.append(f"Top port {top_port.port_name} handled {_fmt_compact(top_port.passengers)} passengers.")

    anomalies = [signal.summary for signal in source_snapshot.signals if signal.severity in {"watch", "alert"}][:3]
    if not anomalies and source_snapshot.signals:
        anomalies = [source_snapshot.signals[0].summary]

    comparisons = [delta.summary for delta in source_snapshot.deltas if delta.summary][:3]

    deterministic_action = source_snapshot.action_candidates[0] if source_snapshot.action_candidates else None
    signal_lookup = {
        **{delta.name: delta.summary for delta in source_snapshot.deltas},
        **{signal.kind: signal.summary for signal in source_snapshot.signals},
    }

    if overlay is None:
        title = "Harbor Intelligence Report Card"
        report_text = _deterministic_brief(source_snapshot)
        recommendations = []
        if deterministic_action is not None:
            recommendations.append(deterministic_action.action)
        recommendations.append(f"Outlook: {_deterministic_outlook(source_snapshot)}")
        insight_text = (
            f"Recommended action rationale: {deterministic_action.rationale}"
            if deterministic_action is not None
            else "Recommended action rationale unavailable."
        )
    else:
        title = overlay.headline
        report_text = overlay.analyst_note
        recommendations = []
        insight_parts = []
        cited_summaries = [signal_lookup[signal_name] for signal_name in overlay.cited_signals if signal_name in signal_lookup]
        if cited_summaries:
            insight_parts.append("Signals: " + "; ".join(cited_summaries[:3]))
        insight_text = " ".join(insight_parts) if insight_parts else None

    return AutoPanelOutput(
        title=title,
        summary=summary,
        facts=facts[:4],
        anomalies=anomalies[:3],
        comparisons=comparisons[:3],
        recommendations=recommendations[:3],
        report_text=report_text,
        insight_text=insight_text,
    )
