from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.intelligence.detector import detect_notable_change
from dashboard.intelligence.generator import generate_auto_panel_snapshot
from dashboard.intelligence.panel import build_data_version
from dashboard.intelligence.writer import upsert_snapshot


def _qualify(project: str, dataset: str, table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) == 3:
        return table_name
    if len(parts) == 2:
        return f"{project}.{parts[0]}.{parts[1]}"
    return f"{project}.{dataset}.{table_name}"


def _run_query(client: bigquery.Client, query: str, params: tuple[tuple[str, str, object], ...] = ()) -> pd.DataFrame:
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(n, t, v) for n, t, v in params]
        ),
    )
    return job.to_dataframe()


def _build_context(daily: pd.DataFrame, routes: pd.DataFrame, ports: pd.DataFrame) -> dict:
    passengers_total = float(pd.to_numeric(daily.get("total_passengers", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    vehicles_total = float(pd.to_numeric(daily.get("total_vehicles", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    rows_total = float(pd.to_numeric(daily.get("rows_count", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())

    top_corridor = "No data"
    top_corridor_passengers = 0.0
    if not routes.empty and {"departure_port", "arrival_port", "total_passengers"}.issubset(routes.columns):
        rr = routes[["departure_port", "arrival_port", "total_passengers"]].copy()
        rr["total_passengers"] = pd.to_numeric(rr["total_passengers"], errors="coerce").fillna(0)
        rr = rr.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"].sum().sort_values("total_passengers", ascending=False)
        if not rr.empty:
            row = rr.iloc[0]
            top_corridor = f"{row['departure_port']} -> {row['arrival_port']}"
            top_corridor_passengers = float(row["total_passengers"])

    top_port = "No data"
    top_port_passengers = 0.0
    if not ports.empty and {"port_name", "total_departing_passengers", "total_arriving_passengers"}.issubset(ports.columns):
        pp = ports[["port_name", "total_departing_passengers", "total_arriving_passengers"]].copy()
        pp["total_departing_passengers"] = pd.to_numeric(pp["total_departing_passengers"], errors="coerce").fillna(0)
        pp["total_arriving_passengers"] = pd.to_numeric(pp["total_arriving_passengers"], errors="coerce").fillna(0)
        pp["total"] = pp["total_departing_passengers"] + pp["total_arriving_passengers"]
        pp = pp.groupby("port_name", as_index=False)["total"].sum().sort_values("total", ascending=False)
        if not pp.empty:
            row = pp.iloc[0]
            top_port = str(row["port_name"])
            top_port_passengers = float(row["total"])

    summary = (
        f"Window summary: {int(passengers_total)} passengers, {int(vehicles_total)} vehicles, "
        f"{int(rows_total)} rows. Top corridor: {top_corridor}. Top port: {top_port}."
    )

    return {
        "summary": summary,
        "facts": [
            f"Total passengers in scope: {int(passengers_total)}.",
            f"Total vehicles in scope: {int(vehicles_total)}.",
            f"Top corridor: {top_corridor} ({int(top_corridor_passengers)} passengers).",
            f"Top port: {top_port} ({int(top_port_passengers)} passengers).",
        ],
        "anomalies": [],
        "comparisons": [
            f"Vehicles per passenger ratio: {(vehicles_total / passengers_total):.3f}." if passengers_total > 0 else "Vehicles per passenger ratio: N/A."
        ],
        "recommendations": [
            "Review major route shifts before weekly operational planning."
        ],
        "report_text": summary,
    }


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env")

    bq_project = os.getenv("THALASSA_BQ_PROJECT", "").strip()
    bq_dataset = os.getenv("THALASSA_BQ_DATASET", "thalassa").strip() or "thalassa"
    bq_location = os.getenv("THALASSA_BQ_LOCATION", "").strip() or None
    insights_table = os.getenv("THALASSA_INTELLIGENCE_TABLE", "thalassa.intelligence_snapshots").strip() or "thalassa.intelligence_snapshots"

    if not bq_project:
        raise ValueError("Set THALASSA_BQ_PROJECT in .env")

    client = bigquery.Client(project=bq_project, location=bq_location)

    daily_table = _qualify(bq_project, bq_dataset, "thalassa.row_counts_daily")
    routes_table = _qualify(bq_project, bq_dataset, "thalassa.fct_route_traffic_daily")
    ports_table = _qualify(bq_project, bq_dataset, "thalassa.fct_port_activity_daily")

    bounds = _run_query(client, f"SELECT MAX(service_date) AS max_date FROM `{daily_table}`")
    if bounds.empty or pd.isna(bounds.iloc[0]["max_date"]):
        raise RuntimeError("row_counts_daily has no data")

    end_date = pd.to_datetime(bounds.iloc[0]["max_date"]).date()
    start_date = end_date - pd.Timedelta(days=89)

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
    context = _build_context(daily, routes, ports)

    payload = generate_auto_panel_snapshot(
        data_version=data_version,
        context=context,
        notable_change=notable,
    )

    target_table = _qualify(bq_project, bq_dataset, insights_table)
    upsert_snapshot(client, table_fqn=target_table, payload=payload)

    print(
        f"Upserted auto_panel snapshot: data_version={payload.data_version} "
        f"mode={payload.meta.generation_mode} model={payload.meta.model_name}"
    )


if __name__ == "__main__":
    main()
