from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class NotableChangeDecision:
    should_trigger_llm: bool
    reasons: list[str]


def detect_notable_change(
    daily_df: pd.DataFrame,
    routes_df: pd.DataFrame,
    *,
    delta_threshold_pct: float = 20.0,
    concentration_threshold_pct: float = 45.0,
    min_recent_avg_passengers: float = 1000.0,
) -> NotableChangeDecision:
    reasons: list[str] = []

    if not daily_df.empty and {"service_date", "total_passengers"}.issubset(daily_df.columns):
        trend_df = daily_df[["service_date", "total_passengers"]].copy()
        trend_df["service_date"] = pd.to_datetime(trend_df["service_date"], errors="coerce")
        trend_df["total_passengers"] = pd.to_numeric(trend_df["total_passengers"], errors="coerce").fillna(0.0)
        trend_df = trend_df.dropna(subset=["service_date"]).sort_values("service_date")

        if len(trend_df) >= 14:
            recent_avg = float(trend_df.tail(7)["total_passengers"].mean())
            prior_avg = float(trend_df.iloc[-14:-7]["total_passengers"].mean())
            if prior_avg > 0:
                delta_pct = ((recent_avg - prior_avg) / prior_avg) * 100.0
                if abs(delta_pct) >= delta_threshold_pct and recent_avg >= min_recent_avg_passengers:
                    reasons.append(
                        f"7-day passenger average changed {delta_pct:+.1f}% vs prior week"
                    )

    if not routes_df.empty and {"departure_port", "arrival_port", "total_passengers"}.issubset(routes_df.columns):
        grouped = routes_df[["departure_port", "arrival_port", "total_passengers"]].copy()
        grouped["total_passengers"] = pd.to_numeric(grouped["total_passengers"], errors="coerce").fillna(0.0)
        grouped = grouped.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"].sum()
        total_passengers = float(grouped["total_passengers"].sum())
        if total_passengers > 0:
            top_share_pct = (float(grouped["total_passengers"].max()) / total_passengers) * 100.0
            if top_share_pct >= concentration_threshold_pct:
                reasons.append(
                    f"Top corridor concentration reached {top_share_pct:.1f}%"
                )

    return NotableChangeDecision(should_trigger_llm=bool(reasons), reasons=reasons)
