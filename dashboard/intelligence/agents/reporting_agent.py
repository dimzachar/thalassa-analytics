"""Deterministic narrative helpers for the dashboard fallback intelligence card."""

from __future__ import annotations

import pandas as pd


def _fmt_compact(value: float | int) -> str:
    """Format large numeric values into compact dashboard-friendly strings."""
    v = float(value)
    a = abs(v)
    if a >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if a >= 100_000:
        return f"{v/1_000:.0f}k"
    if a >= 10_000:
        return f"{v/1_000:.1f}k"
    return f"{v:,.0f}"


def build_deterministic_report_lines(
    *,
    current_period_text: str,
    comparison_baseline_text: str,
    pax_val: float,
    veh_val: float,
    route_rank: pd.DataFrame,
    port_rank: pd.DataFrame,
    congestion: float,
    daily: pd.DataFrame,
    routes: pd.DataFrame,
) -> list[str]:
    """Build the deterministic four-paragraph fallback report for the agent card."""
    top_corridor = route_rank.iloc[0]["pair_label"] if not route_rank.empty else "No data"
    top_port = port_rank.iloc[0]["port_name"] if not port_rank.empty else "No data"
    top_corridor_pax = float(route_rank.iloc[0]["passengers"]) if not route_rank.empty else 0.0
    top_corridor_share = (top_corridor_pax / pax_val) * 100.0 if pax_val > 0 else 0.0

    paragraph_current = (
        f"Current period: {current_period_text}. The network recorded {_fmt_compact(pax_val)} passengers and "
        f"{_fmt_compact(veh_val)} vehicles. The leading corridor is {top_corridor} "
        f"({top_corridor_share:.1f}% of scoped passengers), with {top_port} as the highest-flow port. "
        f"Congestion index is {congestion:.0f}%. Comparison baseline: {comparison_baseline_text}."
    )

    weekly_delta_text = "Not enough history for 7-day change."
    monthly_delta_text = "Not enough history for 30-day change."
    anomaly_hint = "No high-severity anomaly flag triggered in the current scope."
    outlook_hint = "Direction is stable."

    trend_df = pd.DataFrame()
    if not daily.empty and {"service_date", "total_passengers"}.issubset(daily.columns):
        trend_df = daily[["service_date", "total_passengers"]].copy()
        trend_df["service_date"] = pd.to_datetime(trend_df["service_date"], errors="coerce")
        trend_df["total_passengers"] = pd.to_numeric(trend_df["total_passengers"], errors="coerce").fillna(0.0)
        trend_df = trend_df.dropna(subset=["service_date"]).sort_values("service_date")

    if len(trend_df) >= 14:
        recent_avg_7 = float(trend_df.tail(7)["total_passengers"].mean())
        prior_avg_7 = float(trend_df.iloc[-14:-7]["total_passengers"].mean())
        if prior_avg_7 > 0:
            delta_7 = ((recent_avg_7 - prior_avg_7) / prior_avg_7) * 100.0
            weekly_delta_text = f"7-day average passengers are {delta_7:+.1f}% vs prior week."
            if abs(delta_7) >= 20:
                anomaly_hint = (
                    f"Traffic momentum moved {delta_7:+.1f}% week-over-week, which is a notable swing. "
                    "Likely causes include schedule shifts, route substitution, or reporting lag."
                )
            outlook_hint = (
                "Expect a mild upward drift over the next 7-14 days."
                if delta_7 > 5
                else "Expect a mild downward normalization over the next 7-14 days."
                if delta_7 < -5
                else "Expect relatively flat movement over the next 7-14 days."
            )

    if len(trend_df) >= 60:
        recent_avg_30 = float(trend_df.tail(30)["total_passengers"].mean())
        prior_avg_30 = float(trend_df.iloc[-60:-30]["total_passengers"].mean())
        if prior_avg_30 > 0:
            delta_30 = ((recent_avg_30 - prior_avg_30) / prior_avg_30) * 100.0
            monthly_delta_text = f"30-day average passengers are {delta_30:+.1f}% vs prior 30-day window."

    shift_note = "Route rank shift data unavailable."
    if not routes.empty and {"service_date", "departure_port", "arrival_port", "total_passengers"}.issubset(routes.columns):
        rr = routes[["service_date", "departure_port", "arrival_port", "total_passengers"]].copy()
        rr["service_date"] = pd.to_datetime(rr["service_date"], errors="coerce")
        rr["total_passengers"] = pd.to_numeric(rr["total_passengers"], errors="coerce").fillna(0.0)
        rr = rr.dropna(subset=["service_date"])
        if not rr.empty:
            max_day = rr["service_date"].max()
            recent_start = max_day - pd.Timedelta(days=6)
            prior_start = max_day - pd.Timedelta(days=13)
            prior_end = max_day - pd.Timedelta(days=7)
            recent = rr[(rr["service_date"] >= recent_start) & (rr["service_date"] <= max_day)]
            prior = rr[(rr["service_date"] >= prior_start) & (rr["service_date"] <= prior_end)]
            if not recent.empty and not prior.empty:
                recent_rank = (
                    recent.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"]
                    .sum()
                    .sort_values("total_passengers", ascending=False)
                )
                prior_rank = (
                    prior.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"]
                    .sum()
                    .sort_values("total_passengers", ascending=False)
                )
                if not recent_rank.empty and not prior_rank.empty:
                    r_dep = str(recent_rank.iloc[0]["departure_port"])
                    r_arr = str(recent_rank.iloc[0]["arrival_port"])
                    p_dep = str(prior_rank.iloc[0]["departure_port"])
                    p_arr = str(prior_rank.iloc[0]["arrival_port"])
                    recent_top = f"{r_dep} -> {r_arr}"
                    prior_top = f"{p_dep} -> {p_arr}"
                    if recent_top == prior_top:
                        shift_note = f"Top corridor remains {recent_top} across the last two weekly windows."
                    else:
                        shift_note = f"Top corridor shifted from {prior_top} to {recent_top} in the latest week."

    paragraph_shifts = f"Notable shifts: {weekly_delta_text} {monthly_delta_text} {shift_note}"
    paragraph_anomaly = f"Anomaly call-out: {anomaly_hint}"
    paragraph_outlook = (
        f"Outlook: {outlook_hint} This is a short operational directional signal based on recent window behavior; "
        "re-check after the next ingestion snapshot."
    )

    return [paragraph_current, paragraph_shifts, paragraph_anomaly, paragraph_outlook]
