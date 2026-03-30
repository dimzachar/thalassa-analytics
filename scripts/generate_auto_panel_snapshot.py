"""Generate and upsert intelligence snapshots from warehouse traffic tables."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.intelligence.agents.detector_agent import detect_notable_change
from dashboard.intelligence.agents.generation_agent import generate_auto_panel_snapshot
from dashboard.intelligence.bigquery_runtime import create_bigquery_client, job_to_dataframe
from dashboard.intelligence.agents.snapshot_agent import build_auto_panel_source_snapshot
from dashboard.intelligence.repositories import build_data_version, upsert_snapshot
from runtime_config import (
    build_dataset_table,
    get_bq_dataset,
    get_bq_location,
    get_bq_project,
    get_intelligence_table,
    qualify_bigquery_table,
)


def _qualify(project: str, dataset: str, table_name: str) -> str:
    """Qualify a table name into a full BigQuery table identifier."""
    parts = table_name.split(".")
    if len(parts) == 3:
        return table_name
    if len(parts) == 2:
        return f"{project}.{parts[0]}.{parts[1]}"
    return f"{project}.{dataset}.{table_name}"


def _run_query(client: bigquery.Client, query: str, params: tuple[tuple[str, str, object], ...] = ()) -> pd.DataFrame:
    """Run a parameterized BigQuery query and return the results as a DataFrame."""
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(n, t, v) for n, t, v in params]
        ),
    )
    return job_to_dataframe(job)


def main() -> None:
    """Build source data windows, generate snapshots, and upsert them into BigQuery."""
    bq_project = get_bq_project()
    bq_dataset = get_bq_dataset()
    bq_location = get_bq_location()
    insights_table = get_intelligence_table(bq_dataset)
    snapshot_end_override = os.getenv("THALASSA_SNAPSHOT_END_DATE", "").strip()
    lookback_days_override = os.getenv("THALASSA_SNAPSHOT_LOOKBACK_DAYS", "").strip()
    kpi_window_override = os.getenv("THALASSA_KPI_WINDOW_DAYS", "").strip()

    if not bq_project:
        raise ValueError(
            "Unable to resolve BigQuery project. Set THALASSA_BQ_PROJECT "
            "or GOOGLE_CLOUD_PROJECT."
        )

    client = create_bigquery_client(bq_project, bq_location)

    daily_table = qualify_bigquery_table(
        bq_project,
        bq_dataset,
        build_dataset_table("row_counts_daily", bq_dataset),
    )
    routes_table = qualify_bigquery_table(
        bq_project,
        bq_dataset,
        build_dataset_table("fct_route_traffic_daily", bq_dataset),
    )
    ports_table = qualify_bigquery_table(
        bq_project,
        bq_dataset,
        build_dataset_table("fct_port_activity_daily", bq_dataset),
    )

    bounds = _run_query(client, f"SELECT MIN(service_date) AS min_date, MAX(service_date) AS max_date FROM `{daily_table}`")
    if bounds.empty or pd.isna(bounds.iloc[0]["max_date"]):
        raise RuntimeError("row_counts_daily has no data")

    min_available_date = pd.to_datetime(bounds.iloc[0]["min_date"]).date()
    max_available_date = pd.to_datetime(bounds.iloc[0]["max_date"]).date()
    end_date = max_available_date
    if snapshot_end_override:
        try:
            parsed_end = pd.to_datetime(snapshot_end_override).date()
            end_date = min(parsed_end, max_available_date)
        except Exception as exc:
            raise ValueError(f"Invalid THALASSA_SNAPSHOT_END_DATE: {snapshot_end_override}") from exc

    lookback_days = 90
    if lookback_days_override:
        try:
            lookback_days = max(7, int(lookback_days_override))
        except ValueError as exc:
            raise ValueError(f"Invalid THALASSA_SNAPSHOT_LOOKBACK_DAYS: {lookback_days_override}") from exc

    kpi_window_days = 7
    if kpi_window_override:
        try:
            kpi_window_days = max(7, int(kpi_window_override))
        except ValueError as exc:
            raise ValueError(f"Invalid THALASSA_KPI_WINDOW_DAYS: {kpi_window_override}") from exc

    preset_override = os.getenv("THALASSA_SNAPSHOT_PRESETS", "").strip()
    preset_values = [p.strip().lower() for p in preset_override.split(",") if p.strip()] if preset_override else []

    def _resolve_lookback(preset: str) -> int:
        """Resolve a preset token into an explicit snapshot lookback window size."""
        if preset in {"full", "all"}:
            return max(7, int((end_date - min_available_date).days) + 1)
        if preset in {"year", "1y", "365", "last365", "last365d"}:
            return 365
        try:
            return max(7, int(preset))
        except ValueError:
            raise ValueError(f"Invalid preset in THALASSA_SNAPSHOT_PRESETS: {preset}")

    lookbacks = [_resolve_lookback(p) for p in preset_values] if preset_values else [lookback_days]

    for idx, lookback in enumerate(lookbacks):
        effective_lookback = min(lookback, int((end_date - min_available_date).days) + 1)
        start_date = end_date - pd.Timedelta(days=effective_lookback - 1)

        daily = _run_query(
            client,
            f"""
            SELECT service_date, rows_count, total_passengers, total_vehicles
            FROM `{daily_table}`
            WHERE service_date BETWEEN @start_date AND @end_date
            ORDER BY service_date
            """,
            params=(("start_date", "DATE", start_date.isoformat()), ("end_date", "DATE", end_date.isoformat())),
        )
        routes = _run_query(
            client,
            f"""
            SELECT service_date, departure_port, arrival_port, total_passengers
            FROM `{routes_table}`
            WHERE service_date BETWEEN @start_date AND @end_date
            ORDER BY service_date
            """,
            params=(("start_date", "DATE", start_date.isoformat()), ("end_date", "DATE", end_date.isoformat())),
        )
        ports = _run_query(
            client,
            f"""
            SELECT service_date, port_name, total_departing_passengers, total_arriving_passengers
            FROM `{ports_table}`
            WHERE service_date BETWEEN @start_date AND @end_date
            ORDER BY service_date
            """,
            params=(("start_date", "DATE", start_date.isoformat()), ("end_date", "DATE", end_date.isoformat())),
        )

        data_version = build_data_version(daily, routes)
        notable = detect_notable_change(daily, routes)
        source_snapshot = build_auto_panel_source_snapshot(
            daily=daily,
            routes=routes,
            ports=ports,
            notable_change=notable,
            kpi_window_days=kpi_window_days,
        )

        payload = generate_auto_panel_snapshot(
            data_version=data_version,
            source_snapshot=source_snapshot,
            notable_change=notable,
        )

        target_table = qualify_bigquery_table(bq_project, bq_dataset, insights_table)
        upsert_snapshot(client, table_fqn=target_table, payload=payload)

        print(
            f"Upserted auto_panel snapshot: data_version={payload.data_version} "
            f"mode={payload.meta.generation_mode} model={payload.meta.model_name} "
            f"window_days={effective_lookback}"
        )


if __name__ == "__main__":
    main()
