"""Streamlit dashboard for maritime traffic analytics and cached intelligence snapshots."""

from __future__ import annotations

import concurrent.futures
import os
import sys
from html import escape
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from streamlit_option_menu import option_menu

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from intelligence import build_agent_context, build_deterministic_report_lines
from intelligence.bigquery_runtime import create_bigquery_client, job_to_dataframe
from runtime_config import (
    build_dataset_table,
    get_bq_dataset,
    get_bq_location,
    get_bq_project,
    get_intelligence_table,
)

BQ_PROJECT = get_bq_project()
BQ_DATASET = get_bq_dataset()
BQ_LOCATION = get_bq_location()
INTELLIGENCE_TABLE = get_intelligence_table(BQ_DATASET)

if not BQ_PROJECT:
    st.error("Set THALASSA_BQ_PROJECT in `.env` or environment variables.")
    st.stop()


st.set_page_config(
    page_title="Thalassa Grid",
    page_icon="ferry",
    layout="wide",
    initial_sidebar_state="expanded",
)

CHART_AXIS = {
    "labelColor": "#CBD5E1",
    "titleColor": "#E2E8F0",
    "gridColor": "rgba(120,146,184,0.36)",
    "domainColor": "rgba(148,176,214,0.78)",
    "tickColor": "rgba(148,176,214,0.78)",
    "gridDash": [3, 3],
}
CHART_LEGEND = {"labelColor": "#E2E8F0", "titleColor": "#E2E8F0", "labelFontSize": 11}

CHART_SERIES_DOMAIN = ["total_passengers", "total_vehicles", "sailings"]
CHART_SERIES_RANGE = ["#0072B2", "#E69F00", "#009E73"]
CHART_SEQUENTIAL_SCALE = {"scheme": {"name": "cividis", "extent": [0.30, 0.98]}}

CHART_PRIMARY_BAR_COLOR = "#3B82F6"
CHART_SECONDARY_LINE_COLOR = "#7DD3FC"
CHART_ROUTE_BAR_COLOR = "#0072B2"
CHART_PORT_BAR_COLOR = "#009E73"
CHART_WEEKLY_VEHICLE_LINE_COLOR = "#E69F00"
CHART_POINT_STROKE = "#F8FAFC"
CHART_NETFLOW_POS_COLOR = "#D55E00"
CHART_NETFLOW_NEG_COLOR = "#0072B2"

def _inject_styles() -> None:
    """Inject the dashboard CSS styles."""
    st.markdown(
        """
        <style>
        @import url("https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap");
        :root {
            --bg: #0A142A;
            --bg2: #0A142A;
            --sidebar: #0A142A;
            --panel: #151E32;
            --panel2: #151E32;
            --text: #E7EDF8;
            --muted: #8EA2C2;
            --accent: #0EA5E9;
            --success: #10B981;
            --warn: #F59E0B;
            --danger: #EF4444;
            --stroke: rgba(72, 102, 148, 0.52);
            --shadow: 0 12px 34px rgba(3, 10, 25, .42);
        }
        #MainMenu, footer { display: none; }
        .stApp {
            color: var(--text);
            background: linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);
            font-family: "Inter", "Segoe UI", sans-serif;
        }
        /* Keep app content clear of Streamlit's top-right toolbar (Deploy, menu, etc.). */
        .block-container {
            max-width: 1560px;
            padding-top: calc(3.25rem + env(safe-area-inset-top));
            padding-bottom: 2rem;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #091225 0%, #0A142A 100%);
            border-right: 1px solid var(--stroke);
        }
        [data-testid="stSidebar"] > div:first-child { width: 18.4rem; }

        .brand {
            padding: .95rem .2rem 1rem .2rem;
            border-bottom: 1px solid var(--stroke);
            margin-bottom: .85rem;
        }
        .brand-title {
            margin: 0;
            color: var(--text);
            font-size: 1.2rem;
            letter-spacing: .02em;
            font-weight: 700;
        }
        .brand-title span { color: var(--accent); }
        .brand-sub {
            margin-top: .25rem;
            color: var(--muted);
            letter-spacing: .12em;
            text-transform: uppercase;
            font-size: .68rem;
        }
        [data-testid="stSidebar"] .stOptionMenu {
            margin-top: .35rem;
            margin-bottom: .9rem;
        }
        [data-testid="stSidebar"] .stOptionMenu hr {
            display: none;
        }
        [data-testid="stSidebar"] .stOptionMenu > div,
        [data-testid="stSidebar"] .stOptionMenu > div > ul,
        [data-testid="stSidebar"] .stOptionMenu > div > ul > li {
            background: transparent !important;
            border: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            box-shadow: none !important;
        }
        [data-testid="stSidebar"] .stOptionMenu .nav-link {
            transition: all .18s ease;
            letter-spacing: .01em;
            line-height: 1.25;
        }
        [data-testid="stSidebar"] .stOptionMenu .nav-link:hover {
            background: rgba(20,38,72,.92) !important;
            border-color: rgba(92,126,176,.62) !important;
            color: #D7E5FA !important;
            transform: translateY(-1px);
        }

        .topbar {
            display:flex; justify-content:space-between; gap:1rem; align-items:flex-start;
            margin-bottom: .95rem;
        }
        .title { margin:0; font-size: 2.1rem; line-height:1.03; letter-spacing:.01em; }
        .subtitle { margin:.35rem 0 0 0; color: var(--muted); font-size:.95rem; line-height:1.6; }
        .live {
            display:inline-flex; align-items:center; gap:.5rem;
            border:1px solid var(--stroke); border-radius:999px;
            padding:.42rem .8rem; font-size:.72rem; letter-spacing:.12em;
            text-transform:uppercase; background: rgba(255,255,255,.03);
        }
        .dot { width:.5rem; height:.5rem; border-radius:999px; background: var(--success); box-shadow:0 0 10px rgba(16,185,129,.7); }

        .mini-grid { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:.7rem; margin-bottom:.8rem; }
        .mini { border:1px solid var(--stroke); border-radius:.78rem; padding:.8rem .9rem; background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%); }
        .mini .k { font-size:.68rem; text-transform:uppercase; letter-spacing:.13em; color:var(--muted); }
        .mini .v { margin-top:.2rem; font-size:1rem; font-weight:600; }
        .analytics-chips {
            display:grid;
            grid-template-columns: repeat(3, minmax(0,1fr));
            gap:.62rem;
            margin: .25rem 0 .85rem 0;
        }
        .analytics-chip {
            border:1px solid var(--stroke);
            border-radius:.72rem;
            background: rgba(255,255,255,.02);
            padding:.55rem .7rem;
        }
        .analytics-chip .k { font-size:.65rem; letter-spacing:.1em; text-transform:uppercase; color:var(--muted); }
        .analytics-chip .v { margin-top:.2rem; font-size:.98rem; font-weight:700; color:#EAF3FF; }

        .kpi-grid {
            display:grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap:.8rem;
            margin-bottom: 1rem;
        }
        .card {
            height:164px;
            border:1px solid var(--stroke);
            border-radius:.78rem;
            padding:.88rem .95rem .85rem .95rem;
            background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%);
            box-shadow: var(--shadow);
            display:flex;
            flex-direction:column;
        }
        .card-top { display:flex; justify-content:space-between; gap:.6rem; }
        .card-label { font-size:.84rem; color:var(--muted); }
        .card-ico {
            width:1.95rem; height:1.95rem; border-radius:.55rem;
            border:1px solid var(--stroke); display:flex; align-items:center; justify-content:center;
            font-size:.72rem; color:#0EA5E9; background: rgba(255,255,255,.02);
        }
        .card-ico .glyph { font-size: .95rem; line-height: 1; }
        .card-val { margin-top:.95rem; font-size:2rem; line-height:1; font-weight:700; color:#E7EDF8; }
        .card-sub { margin-top:.35rem; color:var(--muted); font-size:.8rem; min-height:1.4em; line-height:1.2; }
        .delta { margin-top:auto; font-size:.8rem; font-weight:700; display:flex; gap:.35rem; align-items:center; justify-content:flex-end; }
        .delta .arr { font-size:.8rem; }
        .delta.up { color: var(--success); }
        .delta.down { color: var(--danger); }
        .delta.flat { color: var(--muted); }

        [data-testid="stVerticalBlockBorderWrapper"] {
            border:1px solid var(--stroke);
            border-radius:.82rem;
            background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%);
            box-shadow: var(--shadow);
            padding: 1.02rem 1.02rem .72rem 1.02rem;
        }
        /* Force full-shell card background for chart panels (KPI-card-like behavior). */
        div[data-testid="stVerticalBlockBorderWrapper"]:has([data-testid="stVegaLiteChart"]),
        div[data-testid="stVerticalBlock"]:has(> div[data-testid="stElementContainer"] [data-testid="stVegaLiteChart"]) {
            background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%) !important;
            border: 1px solid var(--stroke) !important;
            border-radius: .82rem !important;
            box-shadow: var(--shadow) !important;
        }
        div[data-testid="stVerticalBlockBorderWrapper"]:has([data-testid="stVegaLiteChart"]) > div,
        div[data-testid="stVerticalBlock"]:has(> div[data-testid="stElementContainer"] [data-testid="stVegaLiteChart"]) > div {
            background: transparent !important;
        }
        .panel-head { display:flex; justify-content:space-between; gap:.8rem; margin-bottom:.65rem; }
        .panel-title { margin:0; font-size:1.02rem; font-weight:700; letter-spacing:.01em; color:#DDE7F7; }
        .panel-note { margin:.28rem 0 0 0; font-size:.83rem; color:var(--muted); line-height:1.45; }
        [data-testid="stVegaLiteChart"] > div { background: transparent !important; border-radius: .72rem; }
        .agent {
            position: relative;
            border: 1px solid rgba(109, 139, 184, 0.42);
            border-radius: 1rem;
            padding: 1rem 1rem 1.05rem 1rem;
            background:
                radial-gradient(140% 100% at 0% 0%, rgba(14,165,233,.22) 0%, rgba(14,165,233,0) 52%),
                linear-gradient(180deg, rgba(20,35,69,.66), rgba(14,26,54,.52));
            min-height: 320px;
            overflow: hidden;
        }
        .agent-wrap { margin-bottom: .9rem; }
        .agent::after {
            content: "";
            position: absolute;
            inset: 0;
            pointer-events: none;
            background: linear-gradient(120deg, transparent 15%, rgba(125,211,252,.09) 45%, transparent 75%);
            transform: translateX(-100%);
            animation: agent-sheen 8s ease-in-out infinite;
        }
        @keyframes agent-sheen {
            0%, 70%, 100% { transform: translateX(-100%); }
            85% { transform: translateX(100%); }
        }
        .agent-head {
            display: flex;
            align-items: center;
            gap: .72rem;
            margin-bottom: .78rem;
        }
        .agent-icon {
            width: 2rem;
            height: 2rem;
            border-radius: .62rem;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border: 1px solid rgba(125, 211, 252, .45);
            background: rgba(15, 118, 173, .22);
            font-size: .95rem;
            color: #BAE6FD;
        }
        .agent h4 { margin:0; font-size:1.06rem; letter-spacing:.01em; }
        .agent small { color:var(--muted); display:block; margin-top:.1rem; }
        .agent-flags {
            display: flex;
            flex-wrap: wrap;
            gap: .45rem;
            margin-bottom: .82rem;
        }
        .agent-flag {
            border: 1px solid rgba(126, 169, 220, .35);
            border-radius: 999px;
            padding: .28rem .56rem;
            font-size: .7rem;
            color: #D8E6FB;
            background: rgba(21, 43, 83, .5);
            line-height: 1.25;
        }
        .agent-report {
            position: relative;
            z-index: 1;
            display: grid;
            gap: .62rem;
        }
        .agent-report p {
            margin: 0;
            color: #E7EDF8;
            font-size: .87rem;
            line-height: 1.62;
            letter-spacing: .001em;
        }

        .empty { color:var(--muted); font-size:.9rem; line-height:1.55; }
        [data-testid="stDataFrameResizable"] { border:1px solid var(--stroke); border-radius:.85rem; overflow:hidden; }

        @media (max-width: 1100px) {
            .mini-grid { grid-template-columns: 1fr; }
            .analytics-chips { grid-template-columns: 1fr; }
            .topbar { flex-direction:column; }
            .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            .agent { min-height: 300px; }
        }
        @media (max-width: 680px) {
            .block-container { padding-top: calc(3.6rem + env(safe-area-inset-top)); }
            .kpi-grid { grid-template-columns: 1fr; }
            .agent { min-height: 270px; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _qualify(table_name: str) -> str:
    """Turn a table name into a fully qualified BigQuery reference."""
    parts = table_name.split(".")
    if len(parts) == 3:
        return f"`{table_name}`"
    if len(parts) == 2:
        ds, rel = parts
        return f"`{BQ_PROJECT}.{ds}.{rel}`"
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"


def _load_streamlit_secrets_if_present():
    """Return Streamlit secrets only when a secrets.toml file actually exists."""
    candidate_paths = [
        Path.home() / ".streamlit" / "secrets.toml",
        PROJECT_ROOT / ".streamlit" / "secrets.toml",
        Path(__file__).resolve().parent / ".streamlit" / "secrets.toml",
    ]
    if not any(path.exists() for path in candidate_paths):
        return None

    try:
        return st.secrets
    except Exception:
        return None


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Return a cached BigQuery client with explicit auth resolution."""
    streamlit_secrets = _load_streamlit_secrets_if_present()
    return create_bigquery_client(BQ_PROJECT, BQ_LOCATION, streamlit_secrets=streamlit_secrets)


@st.cache_data(show_spinner=False)
def _run_query(
    query: str,
    scalar_params: tuple[tuple[str, str, object], ...] = (),
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """Run a parameterized BigQuery query and return the results as a DataFrame."""
    client = get_bq_client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(name, type_name, value)
            for name, type_name, value in scalar_params
        ]
    )
    job = client.query(query, job_config=job_config)
    try:
        job.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError as exc:
        raise TimeoutError("BigQuery query timed out") from exc
    return job_to_dataframe(job)


@st.cache_data(show_spinner=False)
def load_date_bounds() -> pd.DataFrame:
    """Load the min and max service dates available in the dataset."""
    return _run_query(
        f"""
        SELECT MIN(service_date) AS min_date, MAX(service_date) AS max_date
        FROM {_qualify(build_dataset_table('row_counts_daily', BQ_DATASET))}
        """
    )


@st.cache_data(show_spinner=False)
def load_daily(start_date: str, end_date: str) -> pd.DataFrame:
    """Load daily network totals for the selected window."""
    return _run_query(
        f"""
        SELECT
            service_date,
            rows_count,
            distinct_itineraries,
            distinct_route_pairs,
            distinct_departure_ports,
            distinct_arrival_ports,
            total_passengers,
            total_vehicles
        FROM {_qualify(build_dataset_table('row_counts_daily', BQ_DATASET))}
        WHERE service_date BETWEEN @start_date AND @end_date
        ORDER BY service_date
        """,
        scalar_params=(("start_date", "DATE", start_date), ("end_date", "DATE", end_date)),
    )


@st.cache_data(show_spinner=False)
def load_weekly(start_date: str, end_date: str) -> pd.DataFrame:
    """Load weekly network totals for the selected window."""
    return _run_query(
        f"""
        SELECT
            week_start,
            rows_count,
            total_passengers,
            total_vehicles,
            distinct_route_pairs,
            distinct_departure_ports,
            distinct_arrival_ports
        FROM {_qualify(build_dataset_table('row_counts_weekly', BQ_DATASET))}
        WHERE week_start BETWEEN @start_date AND @end_date
        ORDER BY week_start
        """,
        scalar_params=(("start_date", "DATE", start_date), ("end_date", "DATE", end_date)),
    )


@st.cache_data(show_spinner=False)
def load_monthly(start_date: str, end_date: str) -> pd.DataFrame:
    """Load monthly network totals for the selected window."""
    return _run_query(
        f"""
        SELECT
            year_month,
            rows_count,
            total_passengers,
            total_vehicles,
            distinct_route_pairs,
            distinct_departure_ports,
            distinct_arrival_ports
        FROM {_qualify(build_dataset_table('row_counts_monthly', BQ_DATASET))}
        WHERE year_month BETWEEN @start_date AND @end_date
        ORDER BY year_month
        """,
        scalar_params=(("start_date", "DATE", start_date), ("end_date", "DATE", end_date)),
    )


@st.cache_data(show_spinner=False)
def load_routes(start_date: str, end_date: str) -> pd.DataFrame:
    """Load route-level traffic metrics for the selected window."""
    return _run_query(
        f"""
        SELECT
            service_date,
            departure_port,
            arrival_port,
            traffic_record_count,
            total_passengers,
            total_vehicles
        FROM {_qualify(build_dataset_table('fct_route_traffic_daily', BQ_DATASET))}
        WHERE service_date BETWEEN @start_date AND @end_date
        ORDER BY service_date
        """,
        scalar_params=(("start_date", "DATE", start_date), ("end_date", "DATE", end_date)),
    )


@st.cache_data(show_spinner=False)
def load_ports(start_date: str, end_date: str) -> pd.DataFrame:
    """Load port-level traffic metrics for the selected window."""
    return _run_query(
        f"""
        SELECT
            service_date,
            port_name,
            departure_record_count,
            arrival_record_count,
            total_departing_passengers,
            total_arriving_passengers,
            total_departing_vehicles,
            total_arriving_vehicles
        FROM {_qualify(build_dataset_table('fct_port_activity_daily', BQ_DATASET))}
        WHERE service_date BETWEEN @start_date AND @end_date
        ORDER BY service_date
        """,
        scalar_params=(("start_date", "DATE", start_date), ("end_date", "DATE", end_date)),
    )


@st.cache_data(show_spinner=False)
def load_filter_options() -> tuple[list[str], list[str]]:
    """Load the available route and port options for the sidebar filters."""
    route_df = _run_query(
        f"""
        SELECT DISTINCT departure_port, arrival_port
        FROM {_qualify(build_dataset_table('fct_route_traffic_daily', BQ_DATASET))}
        WHERE departure_port IS NOT NULL AND arrival_port IS NOT NULL
        ORDER BY departure_port, arrival_port
        """
    )
    port_df = _run_query(
        f"""
        SELECT DISTINCT port_name
        FROM {_qualify(build_dataset_table('fct_port_activity_daily', BQ_DATASET))}
        WHERE port_name IS NOT NULL
        ORDER BY port_name
        """
    )
    route_options = ["All routes"] + [
        f"{str(row['departure_port'])} -> {str(row['arrival_port'])}" for _, row in route_df.iterrows()
    ]
    port_options = ["All ports"] + [str(v) for v in port_df["port_name"].tolist()]
    return route_options, port_options


def _resolve_preset_dates(preset: str, max_date: object) -> tuple[object, object]:
    """Convert a sidebar date preset into actual start and end dates."""
    max_date_ts = pd.to_datetime(max_date).date()
    if preset == "Last 30D":
        return max_date_ts - pd.Timedelta(days=29), max_date_ts
    if preset == "Last 90D":
        return max_date_ts - pd.Timedelta(days=89), max_date_ts
    if preset == "YTD":
        return pd.Timestamp(year=max_date_ts.year, month=1, day=1).date(), max_date_ts
    return max_date_ts - pd.Timedelta(days=89), max_date_ts



def _daily_from_routes(routes_df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild daily totals from route rows."""
    if routes_df.empty:
        return pd.DataFrame(columns=["service_date", "rows_count", "distinct_route_pairs", "total_passengers", "total_vehicles"])
    scoped = routes_df.copy()
    scoped["pair_label"] = scoped["departure_port"].fillna("Unknown") + " -> " + scoped["arrival_port"].fillna("Unknown")
    daily_scope = (
        scoped.groupby("service_date", as_index=False)
        .agg(
            rows_count=("traffic_record_count", "sum"),
            distinct_route_pairs=("pair_label", "nunique"),
            total_passengers=("total_passengers", "sum"),
            total_vehicles=("total_vehicles", "sum"),
        )
        .sort_values("service_date")
    )
    return daily_scope


def _ports_from_routes(routes_df: pd.DataFrame) -> pd.DataFrame:
    """Rebuild port-level aggregates from route rows."""
    if routes_df.empty:
        return pd.DataFrame(
            columns=[
                "service_date",
                "port_name",
                "departure_record_count",
                "arrival_record_count",
                "total_departing_passengers",
                "total_arriving_passengers",
                "total_departing_vehicles",
                "total_arriving_vehicles",
            ]
        )

    dep = routes_df[
        ["service_date", "departure_port", "traffic_record_count", "total_passengers", "total_vehicles"]
    ].rename(
        columns={
            "departure_port": "port_name",
            "traffic_record_count": "departure_record_count",
            "total_passengers": "total_departing_passengers",
            "total_vehicles": "total_departing_vehicles",
        }
    )
    dep["arrival_record_count"] = 0
    dep["total_arriving_passengers"] = 0
    dep["total_arriving_vehicles"] = 0

    arr = routes_df[
        ["service_date", "arrival_port", "traffic_record_count", "total_passengers", "total_vehicles"]
    ].rename(
        columns={
            "arrival_port": "port_name",
            "traffic_record_count": "arrival_record_count",
            "total_passengers": "total_arriving_passengers",
            "total_vehicles": "total_arriving_vehicles",
        }
    )
    arr["departure_record_count"] = 0
    arr["total_departing_passengers"] = 0
    arr["total_departing_vehicles"] = 0

    unioned = pd.concat([dep, arr], ignore_index=True)
    port_scope = (
        unioned.groupby(["service_date", "port_name"], as_index=False)[
            [
                "departure_record_count",
                "arrival_record_count",
                "total_departing_passengers",
                "total_arriving_passengers",
                "total_departing_vehicles",
                "total_arriving_vehicles",
            ]
        ]
        .sum()
        .sort_values(["service_date", "port_name"])
    )
    return port_scope


def _port_netflow_from_routes(routes_df: pd.DataFrame) -> pd.DataFrame:
    """Compute net passenger flow per port from route rows."""
    if routes_df.empty:
        return pd.DataFrame(
            columns=[
                "port_name",
                "total_departing_passengers",
                "total_arriving_passengers",
                "total_departing_vehicles",
                "total_arriving_vehicles",
            ]
        )

    departing = (
        routes_df.groupby("departure_port", as_index=False)[["total_passengers", "total_vehicles"]]
        .sum()
        .rename(
            columns={
                "departure_port": "port_name",
                "total_passengers": "total_departing_passengers",
                "total_vehicles": "total_departing_vehicles",
            }
        )
    )
    arriving = (
        routes_df.groupby("arrival_port", as_index=False)[["total_passengers", "total_vehicles"]]
        .sum()
        .rename(
            columns={
                "arrival_port": "port_name",
                "total_passengers": "total_arriving_passengers",
                "total_vehicles": "total_arriving_vehicles",
            }
        )
    )

    port_net = departing.merge(arriving, on="port_name", how="outer").fillna(0)
    port_net["net_flow"] = (
        port_net["total_arriving_passengers"] - port_net["total_departing_passengers"]
    )
    port_net["net_flow_abs"] = port_net["net_flow"].abs()
    # Deterministic ranking avoids bar order flips when values tie near top_n cutoff.
    port_net = port_net.sort_values(
        ["net_flow_abs", "net_flow", "port_name"],
        ascending=[False, False, True],
        kind="mergesort",
    ).drop(columns=["net_flow_abs", "net_flow"])
    return port_net


def _port_netflow_reconciliation(routes_df: pd.DataFrame, ports_df: pd.DataFrame) -> pd.DataFrame:
    """Compare route-derived and port-derived passenger totals by port."""
    from_routes = _port_netflow_from_routes(routes_df).rename(
        columns={
            "total_departing_passengers": "dep_routes",
            "total_arriving_passengers": "arr_routes",
        }
    )[["port_name", "dep_routes", "arr_routes"]]

    if ports_df.empty or "port_name" not in ports_df.columns:
        from_ports = pd.DataFrame(columns=["port_name", "dep_ports", "arr_ports"])
    else:
        from_ports = (
            ports_df.groupby("port_name", as_index=False)[
                ["total_departing_passengers", "total_arriving_passengers"]
            ]
            .sum()
            .rename(columns={"total_departing_passengers": "dep_ports", "total_arriving_passengers": "arr_ports"})
        )[["port_name", "dep_ports", "arr_ports"]]

    merged = from_routes.merge(from_ports, on="port_name", how="outer").fillna(0)
    merged["net_routes"] = merged["arr_routes"] - merged["dep_routes"]
    merged["net_ports"] = merged["arr_ports"] - merged["dep_ports"]
    merged["dep_diff"] = merged["dep_routes"] - merged["dep_ports"]
    merged["arr_diff"] = merged["arr_routes"] - merged["arr_ports"]
    merged["net_diff"] = merged["net_routes"] - merged["net_ports"]
    merged["abs_net_diff"] = merged["net_diff"].abs()
    return merged.sort_values("abs_net_diff", ascending=False)


def _fmt_compact(value: float | int) -> str:
    """Format a large number into a compact string like 1.2M or 45k."""
    v = float(value)
    a = abs(v)
    if a >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if a >= 100_000:
        return f"{v/1_000:.0f}k"
    if a >= 10_000:
        return f"{v/1_000:.1f}k"
    return f"{v:,.0f}"


def _pct_delta(curr: float, baseline: float, suffix: str = "vs Avg") -> tuple[str, str]:
    """Return a formatted delta label and directional class for KPI cards."""
    if baseline == 0:
        return f"0.0% {suffix}", "flat"
    delta = (curr - baseline) / baseline * 100
    if abs(delta) < 0.05:
        return f"0.0% {suffix}", "flat"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}% {suffix}", ("up" if delta > 0 else "down")


def _safe_share(numerator: float, denominator: float) -> float:
    """Divide two numbers safely, returning 0 if the denominator is zero."""
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100


def _window_filter(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, date_col: str = "service_date") -> pd.DataFrame:
    """Return rows within an inclusive date range."""
    return df[(df[date_col] >= start) & (df[date_col] <= end)].copy()


def _compute_window_metrics(
    daily_df: pd.DataFrame,
    routes_df: pd.DataFrame,
    ports_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, float]:
    """Compute the core KPI metrics for a single comparison window."""
    daily_w = _window_filter(daily_df, start, end, "service_date")
    routes_w = _window_filter(routes_df, start, end, "service_date")
    ports_w = _window_filter(ports_df, start, end, "service_date")

    pax = float(daily_w["total_passengers"].sum()) if not daily_w.empty else 0.0
    veh = float(daily_w["total_vehicles"].sum()) if not daily_w.empty else 0.0
    sail = float(daily_w["rows_count"].sum()) if not daily_w.empty else 0.0
    active_corridors = float(daily_w["distinct_route_pairs"].mean()) if not daily_w.empty else 0.0

    if not routes_w.empty:
        top_corridor_pax = float(
            routes_w.groupby(["departure_port", "arrival_port"], as_index=False)["total_passengers"]
            .sum()["total_passengers"]
            .max()
        )
    else:
        top_corridor_pax = 0.0
    top_corridor_share = _safe_share(top_corridor_pax, pax)

    if not ports_w.empty:
        port_rollup = ports_w.groupby("port_name", as_index=False)[
            ["total_departing_passengers", "total_arriving_passengers"]
        ].sum()
        port_rollup["total_port_pax"] = (
            port_rollup["total_departing_passengers"] + port_rollup["total_arriving_passengers"]
        )
        top_port_flow = float(port_rollup["total_port_pax"].max())
    else:
        top_port_flow = 0.0

    return {
        "pax": pax,
        "veh": veh,
        "sail": sail,
        "active_corridors": active_corridors,
        "top_corridor_share": top_corridor_share,
        "top_port_flow": top_port_flow,
    }


def _avg_of_windows(windows: list[dict[str, float]], key: str) -> float:
    """Average a metric across non-empty comparison windows."""
    values = [w.get(key, 0.0) for w in windows]
    values = [v for v in values if v > 0]
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _icon_svg(name: str) -> str:
    """Return the glyph used by a KPI card icon slot."""
    icons = {
        "passengers": "👥",
        "vehicles": "🚗",
        "corridors": "🧭",
        "share": "◔",
        "port": "⚓",
    }
    return icons.get(name, icons["passengers"])


def _section_header(title: str, note: str) -> None:
    """Render a section title with a short explanatory note below it."""
    st.markdown(
        f"""
        <div class="panel-head">
            <div>
                <h3 class="panel-title">{title}</h3>
                <p class="panel-note">{note}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _trend_spec(daily: pd.DataFrame) -> dict:
    """Build the Vega-Lite specification for the traffic pulse trend chart."""
    chart = daily.copy()
    chart["service_date"] = pd.to_datetime(chart["service_date"])
    chart = chart.rename(columns={"service_date": "period", "rows_count": "sailings"})
    chart = chart[["period", "total_passengers", "total_vehicles", "sailings"]]
    melted = chart.melt("period", var_name="series", value_name="value")
    return {
        "height": 340,
        "background": "transparent",
        "data": {"values": melted.to_dict("records")},
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "gridColor": "rgba(120,146,184,0.38)",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "tickCount": 6,
                "labelFontSize": 11,
                "titleFontSize": 11,
                "gridDash": [3, 3],
            },
            "legend": CHART_LEGEND,
        },
        "mark": {
            "type": "line",
            "point": {"filled": True, "size": 42},
            "strokeWidth": 2.7,
            "interpolate": "monotone",
        },
        "encoding": {
            "x": {"field": "period", "type": "temporal", "axis": {"title": None, "format": "%d %b"}},
            "y": {"field": "value", "type": "quantitative", "axis": {"title": None, "format": "~s"}},
            "color": {
                "field": "series",
                "type": "nominal",
                "scale": {
                    "domain": CHART_SERIES_DOMAIN,
                    "range": CHART_SERIES_RANGE,
                },
                "legend": {"title": None, "orient": "top"},
            },
            "tooltip": [
                {"field": "period", "type": "temporal", "title": "Period"},
                {"field": "series", "type": "nominal", "title": "Metric"},
                {"field": "value", "type": "quantitative", "title": "Value", "format": ",.0f"},
            ],
        },
    }


def _bar_spec(df: pd.DataFrame, y_field: str, x_field: str, color: str) -> dict:
    """Build a reusable horizontal bar-chart Vega-Lite specification."""
    return {
        "height": 315,
        "background": "transparent",
        "data": {"values": df.to_dict("records")},
        "mark": {"type": "bar", "cornerRadiusEnd": 8, "color": color},
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "gridColor": "rgba(120,146,184,0.38)",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "tickCount": 5,
                "labelFontSize": 11,
                "titleFontSize": 11,
                "gridDash": [3, 3],
            },
        },
        "encoding": {
            "y": {"field": y_field, "type": "ordinal", "sort": "-x", "axis": {"title": None, "labelLimit": 210}},
            "x": {"field": x_field, "type": "quantitative", "axis": {"title": None, "format": "~s"}},
        },
    }


def _weekday_heatmap_spec(daily: pd.DataFrame) -> dict:
    """Build the weekday heatmap specification from daily passenger totals."""
    d = daily[["service_date", "total_passengers"]].copy()
    d["service_date"] = pd.to_datetime(d["service_date"])
    d["weekday"] = d["service_date"].dt.day_name().str[:3]
    d["year_month"] = d["service_date"].dt.to_period("M").astype(str)
    matrix = (
        d.groupby(["year_month", "weekday"], as_index=False)["total_passengers"]
        .mean()
        .rename(columns={"total_passengers": "avg_passengers"})
    )
    return {
        "height": 360,
        "background": "transparent",
        "data": {"values": matrix.to_dict("records")},
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "labelFontSize": 12,
                "titleFontSize": 12,
            },
        },
        "mark": {"type": "rect", "cornerRadius": 3},
        "encoding": {
            "x": {"field": "weekday", "type": "ordinal", "sort": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], "axis": {"title": None}},
            "y": {"field": "year_month", "type": "ordinal", "axis": {"title": None}},
            "color": {
                "field": "avg_passengers",
                "type": "quantitative",
                "scale": CHART_SEQUENTIAL_SCALE,
                "legend": {"title": "Avg passengers", "orient": "top", "labelFontSize": 12, "titleFontSize": 12},
            },
            "tooltip": [
                {"field": "year_month", "type": "nominal", "title": "Month"},
                {"field": "weekday", "type": "nominal", "title": "Weekday"},
                {"field": "avg_passengers", "type": "quantitative", "title": "Avg passengers", "format": ",.0f"},
            ],
        },
    }


def _focus_concentration_data(
    daily: pd.DataFrame,
    routes: pd.DataFrame,
    top_corridors: int = 3,
    grain: str = "daily",
) -> pd.DataFrame:
    """Aggregate top-corridor concentration over time at the requested grain."""
    if routes.empty or daily.empty:
        return pd.DataFrame(columns=["service_date", "total_passengers", "top_share_pct"])

    def _period_start(date_series: pd.Series, period_grain: str) -> pd.Series:
        """Normalize timestamps to the start of the selected reporting period."""
        dt = pd.to_datetime(date_series)
        if period_grain == "weekly":
            return dt.dt.to_period("W-SUN").dt.start_time
        if period_grain == "monthly":
            return dt.dt.to_period("M").dt.start_time
        return dt.dt.normalize()

    period_grain = str(grain).lower()
    if period_grain not in {"daily", "weekly", "monthly"}:
        period_grain = "daily"

    d = routes.copy()
    d["pair_label"] = d["departure_port"].fillna("Unknown") + " -> " + d["arrival_port"].fillna("Unknown")
    totals = d.groupby("pair_label", as_index=False)["total_passengers"].sum().sort_values("total_passengers", ascending=False)
    leaders = set(totals.head(top_corridors)["pair_label"].tolist())
    top_daily = (
        d[d["pair_label"].isin(leaders)]
        .groupby("service_date", as_index=False)["total_passengers"]
        .sum()
        .rename(columns={"total_passengers": "top_passengers"})
    )
    base = daily[["service_date", "total_passengers"]].copy()
    base["service_date"] = _period_start(base["service_date"], period_grain)
    base = base.groupby("service_date", as_index=False)["total_passengers"].sum()
    top_daily["service_date"] = _period_start(top_daily["service_date"], period_grain)
    top_daily = top_daily.groupby("service_date", as_index=False)["top_passengers"].sum()
    merged = base.merge(top_daily, on="service_date", how="left").fillna({"top_passengers": 0})
    merged["top_share_pct"] = (merged["top_passengers"] / merged["total_passengers"].replace(0, pd.NA) * 100).fillna(0)
    merged = merged.sort_values("service_date")
    smooth_window = {"daily": 7, "weekly": 4, "monthly": 3}[period_grain]
    merged["top_share_pct_smooth"] = merged["top_share_pct"].rolling(window=smooth_window, min_periods=1).mean()
    return merged


def _focus_concentration_spec(concentration_df: pd.DataFrame, grain: str = "daily") -> dict:
    """Build the Vega-Lite specification for the corridor concentration view."""
    period_grain = str(grain).lower()
    if period_grain not in {"daily", "weekly", "monthly"}:
        period_grain = "daily"
    date_title = {"daily": "Date", "weekly": "Week start", "monthly": "Month"}[period_grain]
    smooth_label = {"daily": "7d avg", "weekly": "4w avg", "monthly": "3m avg"}[period_grain]
    axis_format = {"daily": "%d %b", "weekly": "%d %b", "monthly": "%b %Y"}[period_grain]
    return {
        "height": 360,
        "background": "transparent",
        "data": {"values": concentration_df.to_dict("records")},
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "gridColor": "rgba(120,146,184,0.36)",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "tickCount": 6,
                "labelFontSize": 11,
                "titleFontSize": 11,
                "gridDash": [3, 3],
            },
        },
        "layer": [
            {
                "mark": {"type": "bar", "cornerRadiusTopLeft": 5, "cornerRadiusTopRight": 5, "opacity": 0.62, "color": CHART_PRIMARY_BAR_COLOR},
                "encoding": {
                    "x": {"field": "service_date", "type": "temporal", "axis": {"title": None, "format": axis_format}},
                    "y": {"field": "total_passengers", "type": "quantitative", "axis": {"title": "Passengers", "format": "~s"}},
                    "tooltip": [
                        {"field": "service_date", "type": "temporal", "title": date_title},
                        {"field": "total_passengers", "type": "quantitative", "title": "Total passengers", "format": ",.0f"},
                        {"field": "top_share_pct", "type": "quantitative", "title": "Top-3 share %", "format": ".1f"},
                    ],
                },
            },
            {
                "mark": {"type": "line", "interpolate": "monotone", "stroke": CHART_SECONDARY_LINE_COLOR, "strokeWidth": 2.7},
                "encoding": {
                    "x": {"field": "service_date", "type": "temporal"},
                    "y": {"field": "top_share_pct_smooth", "type": "quantitative", "axis": {"title": f"Top-3 share % ({smooth_label})", "orient": "right", "format": ".0f"}},
                },
            },
        ],
        "resolve": {"scale": {"y": "independent"}},
    }


def _corridor_efficiency_spec(top_routes: pd.DataFrame, scope_total_passengers: float) -> dict:
    """Build the corridor efficiency scatter plot spec."""
    d = top_routes.copy()
    if d.empty:
        return {"height": 300, "data": {"values": []}}
    d["passengers_per_sailing"] = (d["passengers"] / d["sailings"].replace(0, pd.NA)).fillna(0)
    d["vehicles_per_sailing"] = (d["vehicles"] / d["sailings"].replace(0, pd.NA)).fillna(0)
    d = d[(d["passengers_per_sailing"] > 0) & (d["vehicles_per_sailing"] > 0)].copy()
    total_pax = float(scope_total_passengers)
    d["share_pct"] = (d["passengers"] / total_pax * 100) if total_pax > 0 else pd.Series(0.0, index=d.index)

    # Preserve exact values for tooltips before jittering positions
    d["pax_per_sailing_exact"] = d["passengers_per_sailing"]
    d["veh_per_sailing_exact"] = d["vehicles_per_sailing"]

    # Apply small deterministic jitter in log space so overlapping corridors
    # (e.g. A→B and B→A with near-identical efficiency) become visually separable.
    import numpy as np
    rng = np.random.default_rng(42)  # fixed seed = stable layout on re-render
    x_log = np.log10(d["passengers_per_sailing"].clip(lower=1e-9))
    y_log = np.log10(d["vehicles_per_sailing"].clip(lower=1e-9))
    x_range = x_log.max() - x_log.min() if x_log.max() != x_log.min() else 1.0
    y_range = y_log.max() - y_log.min() if y_log.max() != y_log.min() else 1.0
    jitter_scale = 0.018  # ~1.8% of axis range in log space
    d["passengers_per_sailing"] = d["passengers_per_sailing"] * 10 ** (
        rng.uniform(-jitter_scale * x_range, jitter_scale * x_range, size=len(d))
    )
    d["vehicles_per_sailing"] = d["vehicles_per_sailing"] * 10 ** (
        rng.uniform(-jitter_scale * y_range, jitter_scale * y_range, size=len(d))
    )
    return {
        "height": 300,
        "background": "transparent",
        "data": {"values": d.to_dict("records")},
        "params": [
            {
                "name": "corridor_select",
                "select": {"type": "point", "on": "click", "clear": "dblclick"},
            }
        ],
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "gridColor": "rgba(120,146,184,0.36)",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "labelFontSize": 12,
                "titleFontSize": 12,
                "gridDash": [3, 3],
            },
        },
        "mark": {
            "type": "point",
            "filled": True,
            "stroke": CHART_POINT_STROKE,
            "strokeWidth": 1.4,
            "cursor": "pointer",
        },
        "encoding": {
            "x": {
                "field": "passengers_per_sailing",
                "type": "quantitative",
                "scale": {"type": "log"},
                "axis": {"title": "Passengers per sailing (log)"},
            },
            "y": {
                "field": "vehicles_per_sailing",
                "type": "quantitative",
                "scale": {"type": "log"},
                "axis": {"title": "Vehicles per sailing (log)"},
            },
            "size": {
                "field": "sailings",
                "type": "quantitative",
                "legend": {"title": "Sailings"},
                # Wider size range helps separate overlapping points visually
                "scale": {"range": [40, 600]},
            },
            "color": {
                "field": "share_pct",
                "type": "quantitative",
                "scale": CHART_SEQUENTIAL_SCALE,
                "legend": {"title": "Share %"},
            },
            "opacity": {
                "condition": {"param": "corridor_select", "empty": True, "value": 0.95},
                "value": 0.15,
            },
            "strokeWidth": {
                # Highlight selected point with a thicker stroke ring
                "condition": {"param": "corridor_select", "empty": False, "value": 3.0},
                "value": 1.4,
            },
            "tooltip": [
                {"field": "pair_label", "type": "nominal", "title": "Corridor"},
                {"field": "pax_per_sailing_exact", "type": "quantitative", "title": "Passengers/sailing", "format": ".2f"},
                {"field": "veh_per_sailing_exact", "type": "quantitative", "title": "Vehicles/sailing", "format": ".2f"},
                {"field": "sailings", "type": "quantitative", "title": "Sailings", "format": ",.0f"},
                {"field": "share_pct", "type": "quantitative", "title": "Share %", "format": ".1f"},
            ],
        },
    }


def _port_netflow_spec(port_rank: pd.DataFrame) -> dict:
    """Build the diverging bar-chart specification for passenger port netflow."""
    d = port_rank[["port_name", "total_departing_passengers", "total_arriving_passengers"]].copy()
    d["net_flow"] = d["total_arriving_passengers"] - d["total_departing_passengers"]
    d["abs_net"] = d["net_flow"].abs()
    d = d.sort_values(
        ["abs_net", "net_flow", "port_name"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    d["rank_order"] = range(1, len(d) + 1)
    max_abs_net = float(d["abs_net"].max()) if not d.empty else 0.0
    min_visible_abs_net = max_abs_net * 0.035
    d["net_flow_display"] = d["net_flow"].astype(float)
    if min_visible_abs_net > 0:
        tiny_mask = (d["abs_net"] > 0) & (d["abs_net"] < min_visible_abs_net)
        d.loc[tiny_mask, "net_flow_display"] = d.loc[tiny_mask, "net_flow"].apply(
            lambda val: min_visible_abs_net if val > 0 else -min_visible_abs_net
        )
    d["x_start_pos"] = 0.0
    d["x_end_pos"] = d["net_flow_display"].clip(lower=0)
    d["x_start_neg"] = d["net_flow_display"].clip(upper=0)
    d["x_end_neg"] = 0.0

    return {
        "height": 300,
        "background": "transparent",
        "data": {"values": d.to_dict("records")},
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#CBD5E1",
                "titleColor": "#E2E8F0",
                "gridColor": "rgba(120,146,184,0.36)",
                "domainColor": "rgba(148,176,214,0.78)",
                "tickColor": "rgba(148,176,214,0.78)",
                "labelFontSize": 11,
                "titleFontSize": 11,
                "gridDash": [3, 3],
            },
        },
        "layer": [
            {
                "mark": {"type": "rule", "color": "#64748B", "strokeWidth": 1.2},
                "encoding": {"x": {"datum": 0}},
            },
            {
                "transform": [{"filter": "datum.net_flow_display >= 0"}],
                "mark": {"type": "bar", "cornerRadiusTopRight": 7, "cornerRadiusBottomRight": 7},
                "encoding": {
                    "y": {
                        "field": "port_name",
                        "type": "ordinal",
                        "sort": {"field": "rank_order", "order": "ascending"},
                        "axis": {"title": None, "labelLimit": 190},
                    },
                    "x": {"field": "x_start_pos", "type": "quantitative", "axis": {"title": "Net flow (Arrive - Depart)", "format": "~s"}},
                    "x2": {"field": "x_end_pos"},
                    "color": {"value": CHART_NETFLOW_POS_COLOR},
                    "tooltip": [
                        {"field": "port_name", "type": "nominal", "title": "Port"},
                        {"field": "net_flow", "type": "quantitative", "title": "Net flow", "format": ",.0f"},
                        {"field": "total_departing_passengers", "type": "quantitative", "title": "Departing", "format": ",.0f"},
                        {"field": "total_arriving_passengers", "type": "quantitative", "title": "Arriving", "format": ",.0f"},
                    ],
                },
            },
            {
                "transform": [{"filter": "datum.net_flow_display < 0"}],
                "mark": {"type": "bar", "cornerRadiusTopLeft": 7, "cornerRadiusBottomLeft": 7},
                "encoding": {
                    "y": {
                        "field": "port_name",
                        "type": "ordinal",
                        "sort": {"field": "rank_order", "order": "ascending"},
                        "axis": {"title": None, "labelLimit": 190},
                    },
                    "x": {"field": "x_start_neg", "type": "quantitative", "axis": {"title": "Net flow (Arrive - Depart)", "format": "~s"}},
                    "x2": {"field": "x_end_neg"},
                    "color": {"value": CHART_NETFLOW_NEG_COLOR},
                    "tooltip": [
                        {"field": "port_name", "type": "nominal", "title": "Port"},
                        {"field": "net_flow", "type": "quantitative", "title": "Net flow", "format": ",.0f"},
                        {"field": "total_departing_passengers", "type": "quantitative", "title": "Departing", "format": ",.0f"},
                        {"field": "total_arriving_passengers", "type": "quantitative", "title": "Arriving", "format": ",.0f"},
                    ],
                },
            },
        ],
    }


def _weekday_profile_verify(daily: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    """Compute weekday traffic profile stats for the verification view."""
    weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    empty = pd.DataFrame(columns=["weekday", "avg_passengers", "day_count", "total_passengers"])
    if daily.empty:
        return empty, {
            "start": "N/A",
            "end": "N/A",
            "days": 0,
            "total_pax": 0.0,
            "sunday_avg": 0.0,
        }

    d = daily[["service_date", "total_passengers"]].copy()
    d["service_date"] = pd.to_datetime(d["service_date"])
    d["total_passengers"] = pd.to_numeric(d["total_passengers"], errors="coerce").fillna(0.0)

    daily_totals = (
        d.groupby("service_date", as_index=False)["total_passengers"]
        .sum()
        .sort_values("service_date")
    )
    daily_totals["weekday"] = daily_totals["service_date"].dt.day_name().str[:3]

    weekday_stats = (
        daily_totals.groupby("weekday", as_index=False)
        .agg(
            avg_passengers=("total_passengers", "mean"),
            day_count=("service_date", "nunique"),
            total_passengers=("total_passengers", "sum"),
        )
        .set_index("weekday")
        .reindex(weekday_order)
        .reset_index()
    )

    weekday_stats["avg_passengers"] = pd.to_numeric(weekday_stats["avg_passengers"], errors="coerce").fillna(0.0)
    weekday_stats["total_passengers"] = pd.to_numeric(weekday_stats["total_passengers"], errors="coerce").fillna(0.0)
    weekday_stats["day_count"] = pd.to_numeric(weekday_stats["day_count"], errors="coerce").fillna(0).astype("int64")
    sunday_row = weekday_stats.loc[weekday_stats["weekday"] == "Sun", "avg_passengers"]
    sunday_avg = float(sunday_row.iloc[0]) if not sunday_row.empty else 0.0

    summary: dict[str, object] = {
        "start": daily_totals["service_date"].min().date().isoformat(),
        "end": daily_totals["service_date"].max().date().isoformat(),
        "days": int(daily_totals["service_date"].nunique()),
        "total_pax": float(daily_totals["total_passengers"].sum()),
        "sunday_avg": sunday_avg,
    }
    return weekday_stats, summary


def _timeseries_verify(
    df: pd.DataFrame,
    *,
    date_col: str,
    passengers_col: str,
    vehicles_col: str,
    rows_col: str,
) -> dict[str, object]:
    """Summarize a time series so chart totals can be cross-checked in the UI."""
    if df.empty or date_col not in df.columns:
        return {
            "start": "N/A",
            "end": "N/A",
            "points": 0,
            "passengers": 0.0,
            "vehicles": 0.0,
            "sailings": 0.0,
        }

    scoped = df.copy()
    scoped[date_col] = pd.to_datetime(scoped[date_col], errors="coerce")
    scoped = scoped.dropna(subset=[date_col])
    for col in [passengers_col, vehicles_col, rows_col]:
        if col in scoped.columns:
            scoped[col] = pd.to_numeric(scoped[col], errors="coerce").fillna(0.0)
        else:
            scoped[col] = 0.0

    return {
        "start": scoped[date_col].min().date().isoformat(),
        "end": scoped[date_col].max().date().isoformat(),
        "points": int(scoped[date_col].nunique()),
        "passengers": float(scoped[passengers_col].sum()),
        "vehicles": float(scoped[vehicles_col].sum()),
        "sailings": float(scoped[rows_col].sum()),
    }




def _render_deepen_insight(chart_id: str, snapshot: object | None) -> None:
    """Render the optional expandable deepen-insight block for a chart."""
    state_key = f"show_deepen_{chart_id}"
    if st.button("Deepen this chart", key=f"btn_{state_key}"):
        st.session_state[state_key] = not bool(st.session_state.get(state_key, False))

    if not st.session_state.get(state_key, False):
        return

    if snapshot is None:
        st.caption("No cached deepen insight found for current data version.")
        return

    header = snapshot.title
    if getattr(snapshot, "generated_at", None):
        header = f"{header} | {snapshot.generated_at}"

    lines = [str(line).strip() for line in getattr(snapshot, "lines", []) if str(line).strip()]
    if not lines:
        st.caption("Cached deepen insight is empty.")
        return

    st.caption(header)
    st.markdown("\n".join([f"- {escape(line)}" for line in lines]), unsafe_allow_html=True)


def _agent_mode_label(mode: str | None) -> str:
    """Convert an internal generation mode key to a short readable label."""
    labels = {
        "llm_overlay": "LLM report",
        "deterministic_skip": "Deterministic report",
        "deterministic_no_api_key": "Deterministic report",
        "deterministic_llm_error": "Deterministic report",
        "deterministic_sql": "Deterministic report",
    }
    return labels.get(str(mode or "").strip(), "Report")


def _format_snapshot_age(hours_since: float | None) -> str:
    """Format snapshot freshness as a short relative-age label."""
    if hours_since is None:
        return "Snapshot unavailable"
    minutes = max(1, int(round(hours_since * 60)))
    if minutes < 60:
        return f"Updated {minutes}m ago"
    return f"Updated {hours_since:.1f}h ago"


_inject_styles()

try:
    bounds_df = load_date_bounds()
except NotFound as exc:
    st.error("BigQuery tables not found. Check project/dataset settings.")
    st.caption(str(exc))
    st.stop()
except Exception as exc:
    st.error(f"BigQuery connection failed: {exc}")
    st.stop()

if bounds_df.empty or bounds_df["min_date"].isna().all() or bounds_df["max_date"].isna().all():
    st.warning("No data found in reporting tables.")
    st.stop()

min_date = pd.to_datetime(bounds_df.iloc[0]["min_date"]).date()
max_date = pd.to_datetime(bounds_df.iloc[0]["max_date"]).date()
default_start = max(min_date, max_date - pd.Timedelta(days=89))
default_end = max_date
analytics_preset_options = ["Last 30D", "Last 90D", "YTD", "Custom"]

try:
    route_options, port_options = load_filter_options()
except Exception:
    route_options, port_options = ["All routes"], ["All ports"]


with st.sidebar:
    st.markdown('<div class="brand"><h3 class="brand-title">THALASSA<span>GRID</span></h3><div class="brand-sub">MARITIME INTEL PLATFORM</div></div>', unsafe_allow_html=True)
    qp = st.query_params
    current_page_raw = qp.get("page", "dashboard")
    if isinstance(current_page_raw, list):
        current_page_raw = current_page_raw[0] if current_page_raw else "dashboard"
    current_page = str(current_page_raw).lower()
    if current_page not in {"dashboard", "analytics"}:
        current_page = "dashboard"

    if "active_page" not in st.session_state:
        st.session_state["active_page"] = current_page
    if current_page != st.session_state["active_page"]:
        st.session_state["active_page"] = current_page
    selected_index = 0 if st.session_state["active_page"] == "dashboard" else 1

    qp_preset_raw = qp.get("preset")
    qp_preset = qp_preset_raw[0] if isinstance(qp_preset_raw, list) else qp_preset_raw

    page_choice = option_menu(
        menu_title=None,
        options=["Dashboard", "Analytics"],
        icons=["activity", "bar-chart-line"],
        menu_icon=None,
        default_index=selected_index,
        orientation="vertical",
        manual_select=selected_index,
        key="page_nav",
        styles={
            "container": {
                "padding": "0!important",
                "margin": "0!important",
                "background-color": "#0B1120",
                "border": "0",
                "border-radius": "0",
                "box-shadow": "none",
            },
            "icon": {
                "color": "#7F8DB4",
                "font-size": "0.9rem",
            },
            "nav-link": {
                "font-size": "0.88rem",
                "font-weight": "600",
                "text-align": "left",
                "margin": "0 0 .45rem 0",
                "padding": ".72rem .86rem",
                "border-radius": ".62rem",
                "color": "#C8D8F0",
                "background-color": "transparent",
                "border": "1px solid rgba(72,102,148,.42)",
            },
            "nav-link-selected": {
                "background-color": "rgba(14,165,233,.16)",
                "color": "#2BC4FF",
                "border": "1px solid rgba(43,196,255,.46)",
            },
        },
    )

    active_page = "dashboard" if page_choice == "Dashboard" else "analytics"
    if active_page != st.session_state["active_page"]:
        st.session_state["active_page"] = active_page
        st.query_params["page"] = active_page
        st.rerun()
    if current_page != st.session_state["active_page"]:
        st.query_params["page"] = st.session_state["active_page"]

    active_page = st.session_state["active_page"]

    qp_start_raw = qp.get("start")
    qp_end_raw = qp.get("end")
    qp_start = qp_start_raw[0] if isinstance(qp_start_raw, list) else qp_start_raw
    qp_end = qp_end_raw[0] if isinstance(qp_end_raw, list) else qp_end_raw
    qp_grain_raw = qp.get("grain")
    qp_grain = qp_grain_raw[0] if isinstance(qp_grain_raw, list) else qp_grain_raw
    qp_top_n_raw = qp.get("top_n")
    qp_top_n = qp_top_n_raw[0] if isinstance(qp_top_n_raw, list) else qp_top_n_raw
    qp_route_raw = qp.get("route")
    qp_route = qp_route_raw[0] if isinstance(qp_route_raw, list) else qp_route_raw
    qp_port_raw = qp.get("port")
    qp_port = qp_port_raw[0] if isinstance(qp_port_raw, list) else qp_port_raw

    if "applied_filters" not in st.session_state:
        init_preset = str(qp_preset or "Last 90D")
        if init_preset not in analytics_preset_options:
            init_preset = "Last 90D"

        init_start, init_end = _resolve_preset_dates(init_preset, max_date)
        if init_preset == "Custom" and qp_start and qp_end:
            try:
                init_start = pd.to_datetime(qp_start).date()
                init_end = pd.to_datetime(qp_end).date()
            except Exception:
                init_start, init_end = default_start, default_end

        init_start = max(min_date, min(init_start, max_date))
        init_end = max(min_date, min(init_end, max_date))
        if init_start > init_end:
            init_start, init_end = init_end, init_start

        init_grain = str(qp_grain or "weekly").lower()
        if init_grain not in {"daily", "weekly", "monthly"}:
            init_grain = "weekly"

        try:
            init_top_n = int(qp_top_n) if qp_top_n is not None else 10
        except ValueError:
            init_top_n = 10
        init_top_n = max(5, min(20, init_top_n))

        init_route = str(qp_route or "All routes")
        if init_route not in route_options:
            init_route = "All routes"
        init_port = str(qp_port or "All ports")
        if init_port not in port_options:
            init_port = "All ports"

        st.session_state["applied_filters"] = {
            "preset": init_preset,
            "start": init_start,
            "end": init_end,
            "grain": init_grain,
            "top_n": init_top_n,
            "route": init_route,
            "port": init_port,
        }

    current_filters = st.session_state["applied_filters"]

    if active_page == "analytics":
        with st.form("global_filters"):
            date_preset = st.selectbox(
                "Date preset",
                options=analytics_preset_options,
                index=analytics_preset_options.index(current_filters.get("preset", "Last 90D")),
            )

            if date_preset == "Custom":
                form_date_range = st.date_input(
                    "Date range",
                    value=(current_filters["start"], current_filters["end"]),
                    min_value=min_date,
                    max_value=max_date,
                )
            else:
                preset_start, preset_end = _resolve_preset_dates(date_preset, max_date)
                form_date_range = (preset_start, preset_end)
                st.caption(f"Preset window: {preset_start.strftime('%d %b %Y')} -> {preset_end.strftime('%d %b %Y')}")

            grain = st.selectbox("Time grain", options=["daily", "weekly", "monthly"], index=["daily", "weekly", "monthly"].index(current_filters["grain"]))
            route_filter = st.selectbox("Route", options=route_options, index=route_options.index(current_filters["route"]) if current_filters["route"] in route_options else 0)
            port_filter = st.selectbox("Port", options=port_options, index=port_options.index(current_filters["port"]) if current_filters["port"] in port_options else 0)
            top_n = st.slider("Ranking depth", min_value=5, max_value=20, value=int(current_filters["top_n"]))
            apply_filters = st.form_submit_button("Apply")

        if apply_filters:
            if isinstance(form_date_range, tuple) and len(form_date_range) == 2:
                form_start, form_end = form_date_range
            else:
                form_start, form_end = default_start, default_end
            form_start = max(min_date, min(form_start, max_date))
            form_end = max(min_date, min(form_end, max_date))
            if form_start > form_end:
                form_start, form_end = form_end, form_start
            st.session_state["applied_filters"] = {
                "preset": date_preset,
                "start": form_start,
                "end": form_end,
                "grain": grain,
                "top_n": top_n,
                "route": route_filter,
                "port": port_filter,
            }
            st.query_params["preset"] = date_preset
            st.query_params["start"] = form_start.isoformat()
            st.query_params["end"] = form_end.isoformat()
            st.query_params["grain"] = grain
            st.query_params["top_n"] = str(top_n)
            st.query_params["route"] = route_filter
            st.query_params["port"] = port_filter
            st.rerun()
    else:
        pass

applied_filters = st.session_state["applied_filters"]
if active_page == "dashboard":
    selected_start = default_start
    selected_end = default_end
    top_n = 10
    selected_grain = "daily"
    selected_route = "All routes"
    selected_port = "All ports"
else:
    selected_start = applied_filters["start"]
    selected_end = applied_filters["end"]
    top_n = int(applied_filters["top_n"])
    selected_grain = str(applied_filters["grain"]).lower()
    selected_route = str(applied_filters["route"])
    selected_port = str(applied_filters["port"])

try:
    daily = load_daily(selected_start.isoformat(), selected_end.isoformat())
    weekly = load_weekly(selected_start.isoformat(), selected_end.isoformat())
    monthly = load_monthly(selected_start.isoformat(), selected_end.isoformat())
    routes = load_routes(selected_start.isoformat(), selected_end.isoformat())
    ports = load_ports(selected_start.isoformat(), selected_end.isoformat())
except Exception as exc:
    st.error(f"Failed to load dashboard data: {exc}")
    st.stop()

for col in ["service_date"]:
    if col in daily.columns:
        daily[col] = pd.to_datetime(daily[col])
if "week_start" in weekly.columns:
    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
if "year_month" in monthly.columns:
    monthly["year_month"] = pd.to_datetime(monthly["year_month"])
if "service_date" in routes.columns:
    routes["service_date"] = pd.to_datetime(routes["service_date"])
if "service_date" in ports.columns:
    ports["service_date"] = pd.to_datetime(ports["service_date"])

if active_page == "analytics":
    if selected_route != "All routes" and " -> " in selected_route:
        dep_port, arr_port = selected_route.split(" -> ", 1)
        routes = routes[(routes["departure_port"] == dep_port) & (routes["arrival_port"] == arr_port)].copy()

    if selected_port != "All ports":
        routes = routes[
            (routes["departure_port"] == selected_port) | (routes["arrival_port"] == selected_port)
        ].copy()

    # Analytics KPIs/charts should always share the same scoped base derived from routes.
    daily = _daily_from_routes(routes)
    ports = _ports_from_routes(routes)

if daily.empty:
    st.warning("No rows returned for selected range.")
    st.stop()

last7 = daily.tail(7)

current_end = pd.to_datetime(daily["service_date"]).max()
current_start = current_end - pd.Timedelta(days=6)
current_metrics = _compute_window_metrics(daily, routes, ports, current_start, current_end)

baseline_windows: list[dict[str, float]] = []
baseline_ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
anchor_end = current_start - pd.Timedelta(days=1)
data_min = pd.to_datetime(daily["service_date"]).min()
view_start = pd.to_datetime(selected_start)
view_end = pd.to_datetime(selected_end)

if anchor_end >= view_start:
    baseline_full_weeks_start = anchor_end - pd.Timedelta(days=55)
    if baseline_full_weeks_start >= view_start:
        for i in range(8):
            win_end = anchor_end - pd.Timedelta(days=7 * i)
            win_start = win_end - pd.Timedelta(days=6)
            if win_start < view_start:
                continue
            baseline_ranges.append((win_start, win_end))
            baseline_windows.append(_compute_window_metrics(daily, routes, ports, win_start, win_end))
    else:
        baseline_ranges.append((view_start, anchor_end))
        baseline_windows.append(_compute_window_metrics(daily, routes, ports, view_start, anchor_end))

pax_val = current_metrics["pax"]
veh_val = current_metrics["veh"]
sail_val = current_metrics["sail"]
active_corridors_val = current_metrics["active_corridors"]
top_corridor_share = current_metrics["top_corridor_share"]
top_port_flow = current_metrics["top_port_flow"]

pax_avg = _avg_of_windows(baseline_windows, "pax")
veh_avg = _avg_of_windows(baseline_windows, "veh")
active_corridors_avg = _avg_of_windows(baseline_windows, "active_corridors")
top_corridor_share_avg = _avg_of_windows(baseline_windows, "top_corridor_share")
top_port_flow_avg = _avg_of_windows(baseline_windows, "top_port_flow")
sail_avg = _avg_of_windows(baseline_windows, "sail")
reference_peak = float(daily["rows_count"].quantile(0.95)) if not daily.empty else 0.0
congestion = min(_safe_share(float(last7["rows_count"].mean()) if not last7.empty else 0.0, reference_peak), 100.0)
current_period_text = f"{current_start.strftime('%d %b %Y')} -> {current_end.strftime('%d %b %Y')}"
if baseline_ranges:
    if len(baseline_ranges) >= 8:
        comparison_baseline_text = (
            f"{len(baseline_ranges)} x 7-day windows: "
            f"{baseline_ranges[-1][0].strftime('%d %b %Y')} -> {baseline_ranges[0][1].strftime('%d %b %Y')}"
        )
    else:
        comparison_baseline_text = (
            f"{baseline_ranges[0][0].strftime('%d %b %Y')} -> "
            f"{baseline_ranges[0][1].strftime('%d %b %Y')}"
        )
else:
    comparison_baseline_text = "No comparison baseline available"

metric_tooltips = {
    "Passenger Throughput": f"Sum(total_passengers) in Current Period ({current_period_text}). Delta compares to the comparison baseline.",
    "Vehicle Transit": f"Sum(total_vehicles) in Current Period ({current_period_text}). Delta compares to the comparison baseline.",
    "Active Corridors": f"Mean daily distinct route-pairs in Current Period ({current_period_text}). Delta compares to baseline windows.",
    "Top Corridor Share": f"Top corridor passengers / total passengers in Current Period ({current_period_text}).",
    "Port Flow Leader": f"Max port (departing + arriving passengers) in Current Period ({current_period_text}).",
}

card_data = [
    ("Passenger Throughput", _fmt_compact(pax_val), "Network demand", *_pct_delta(pax_val, pax_avg), _icon_svg("passengers")),
    ("Vehicle Transit", _fmt_compact(veh_val), "Vehicle movement", *_pct_delta(veh_val, veh_avg), _icon_svg("vehicles")),
    ("Active Corridors", f"{active_corridors_val:.0f}", "Route-pair activity", *_pct_delta(active_corridors_val, active_corridors_avg, "vs Baseline"), _icon_svg("corridors")),
    ("Top Corridor Share", f"{top_corridor_share:.1f}%", "Leading OD concentration", *_pct_delta(top_corridor_share, top_corridor_share_avg), _icon_svg("share")),
    ("Port Flow Leader", _fmt_compact(top_port_flow), "Top port passengers", *_pct_delta(top_port_flow, top_port_flow_avg), _icon_svg("port")),
]

route_rank = (
    routes.groupby(["departure_port", "arrival_port"], as_index=False)[["traffic_record_count", "total_passengers", "total_vehicles"]]
    .sum()
    .rename(columns={"traffic_record_count": "sailings", "total_passengers": "passengers", "total_vehicles": "vehicles"})
    .sort_values(["passengers", "vehicles"], ascending=False)
)
route_rank["pair_label"] = route_rank["departure_port"].fillna("Unknown") + " -> " + route_rank["arrival_port"].fillna("Unknown")
route_scope_total_passengers = float(route_rank["passengers"].sum()) if not route_rank.empty else 0.0
top_routes = route_rank.head(top_n).copy()

port_rank = (
    ports.groupby("port_name", as_index=False)[["total_departing_passengers", "total_arriving_passengers", "total_departing_vehicles", "total_arriving_vehicles"]]
    .sum()
    .sort_values(["total_departing_passengers", "total_arriving_passengers"], ascending=False)
)

routes_kpi = routes[
    (routes["service_date"] >= pd.Timestamp(current_start)) & (routes["service_date"] <= pd.Timestamp(current_end))
].copy()
ports_kpi = ports[
    (ports["service_date"] >= pd.Timestamp(current_start)) & (ports["service_date"] <= pd.Timestamp(current_end))
].copy()

route_rank_kpi = (
    routes_kpi.groupby(["departure_port", "arrival_port"], as_index=False)[["traffic_record_count", "total_passengers", "total_vehicles"]]
    .sum()
    .rename(columns={"traffic_record_count": "sailings", "total_passengers": "passengers", "total_vehicles": "vehicles"})
    .sort_values(["passengers", "vehicles"], ascending=False)
)
route_rank_kpi["pair_label"] = route_rank_kpi["departure_port"].fillna("Unknown") + " -> " + route_rank_kpi["arrival_port"].fillna("Unknown")

port_rank_kpi = (
    ports_kpi.groupby("port_name", as_index=False)[["total_departing_passengers", "total_arriving_passengers", "total_departing_vehicles", "total_arriving_vehicles"]]
    .sum()
    .sort_values(["total_departing_passengers", "total_arriving_passengers"], ascending=False)
)

corridor_focus = _focus_concentration_data(daily, routes, top_corridors=3, grain=selected_grain)
top_routes_scatter = route_rank.head(top_n).copy()  # respects ranking depth slider
top_ports_net = _port_netflow_from_routes(routes).head(top_n).copy()

agent_context = build_agent_context(
    _run_query,
    _qualify,
    table_name=INTELLIGENCE_TABLE,
    daily_df=daily,
    routes_df=routes,
    filters={
        "start": selected_start.isoformat(),
        "end": selected_end.isoformat(),
        "grain": selected_grain,
        "route": selected_route,
        "port": selected_port,
    },
    stale_after_hours=48,
)

agent_snapshot = agent_context.panel_snapshot
agent_snapshot_freshness = agent_context.freshness
notable_change = agent_context.notable_change
snapshot_range_match = False
if agent_snapshot is not None and agent_snapshot.kpi_start and agent_snapshot.kpi_end:
    try:
        snapshot_start = pd.to_datetime(agent_snapshot.kpi_start).date()
        snapshot_end = pd.to_datetime(agent_snapshot.kpi_end).date()
        snapshot_range_match = snapshot_start == current_start.date() and snapshot_end == current_end.date()
    except Exception:
        snapshot_range_match = False

use_cached_auto_panel = (
    active_page == "dashboard"
    and selected_route == "All routes"
    and selected_port == "All ports"
    and snapshot_range_match
)

agent_card_title = "Maritime Intelligence"
agent_subtitle = "Deterministic report"
agent_lines = build_deterministic_report_lines(
    current_period_text=current_period_text,
    comparison_baseline_text=comparison_baseline_text,
    pax_val=pax_val,
    veh_val=veh_val,
    route_rank=route_rank_kpi,
    port_rank=port_rank_kpi,
    congestion=congestion,
    daily=daily,
    routes=routes,
)

if use_cached_auto_panel and agent_snapshot is not None:
    agent_card_title = agent_snapshot.title or "Maritime Intelligence"
    agent_lines = agent_snapshot.lines
    subtitle_parts = [_agent_mode_label(agent_snapshot.generation_mode)]
    if agent_snapshot.generated_at:
        subtitle_parts.append(f"Generated {agent_snapshot.generated_at}")
    agent_subtitle = " | ".join(subtitle_parts)

agent_report_paragraphs = [line for line in agent_lines if str(line).strip()]
agent_report_html = "".join(f"<p>{escape(line)}</p>" for line in agent_report_paragraphs)

# TODO: deepen_chart feature not yet implemented
# deepen_traffic_pulse_snapshot = agent_context.deepen_traffic_pulse_snapshot
# deepen_top_corridors_snapshot = agent_context.deepen_top_corridors_snapshot
# deepen_port_balance_snapshot = agent_context.deepen_port_balance_snapshot


if active_page == "dashboard":
    st.markdown(
        f"""
        <div class="topbar">
            <div class="live"><span class="dot"></span><span>Live</span></div>
        </div>
        <div class="mini-grid">
            <div class="mini"><div class="k">Coverage</div><div class="v">{min_date.strftime('%d %b %Y')} -> {max_date.strftime('%d %b %Y')}</div></div>
            <div class="mini"><div class="k">View Window</div><div class="v">{selected_start.strftime('%d %b %Y')} -> {selected_end.strftime('%d %b %Y')}</div></div>
            <div class="mini"><div class="k">Comparison Baseline</div><div class="v">{escape(comparison_baseline_text)}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    kpi_cards_html = ['<div class="kpi-grid">']
    for label, value, sub, delta_text, delta_class, icon_svg in card_data:
        arrow = "▲" if delta_class == "up" else ("▼" if delta_class == "down" else "•")
        tooltip_text = metric_tooltips.get(label, sub)
        kpi_cards_html.append(
            (
                f'<div class="card" title="{escape(tooltip_text)}">'
                '<div class="card-top">'
                f'<span class="card-label">{escape(label)}</span>'
                f'<span class="card-ico"><span class="glyph">{escape(icon_svg)}</span></span>'
                "</div>"
                f'<div class="card-val">{escape(value)}</div>'
                f'<div class="card-sub">{escape(sub)}</div>'
                f'<div class="delta {escape(delta_class)}"><span class="arr">{arrow}</span><span>{escape(delta_text)}</span></div>'
                "</div>"
            )
        )
    kpi_cards_html.append("</div>")
    st.markdown("".join(kpi_cards_html), unsafe_allow_html=True)

if active_page == "dashboard":
    left, right = st.columns((1.75, 1), gap="large")

    with left:
        with st.container(border=True):
            _section_header("Traffic Pulse", f"{selected_grain.title()} trend for the selected View Window.")
            if selected_grain == "daily":
                pulse_df = daily[["service_date", "rows_count", "total_passengers", "total_vehicles"]].copy()
                st.vega_lite_chart(_trend_spec(daily), width="stretch")
                verify = _timeseries_verify(
                    daily,
                    date_col="service_date",
                    passengers_col="total_passengers",
                    vehicles_col="total_vehicles",
                    rows_col="rows_count",
                )
            elif selected_grain == "weekly":
                if weekly.empty:
                    st.markdown('<div class="empty">No weekly rows in selected range. Showing daily fallback.</div>', unsafe_allow_html=True)
                    pulse_df = daily[["service_date", "rows_count", "total_passengers", "total_vehicles"]].copy()
                    st.vega_lite_chart(_trend_spec(daily), width="stretch")
                    verify = _timeseries_verify(
                        daily,
                        date_col="service_date",
                        passengers_col="total_passengers",
                        vehicles_col="total_vehicles",
                        rows_col="rows_count",
                    )
                else:
                    weekly_pulse = weekly.rename(columns={"week_start": "service_date"})[
                        ["service_date", "rows_count", "total_passengers", "total_vehicles"]
                    ].copy()
                    pulse_df = weekly_pulse
                    st.vega_lite_chart(_trend_spec(weekly_pulse), width="stretch")
                    verify = _timeseries_verify(
                        weekly_pulse,
                        date_col="service_date",
                        passengers_col="total_passengers",
                        vehicles_col="total_vehicles",
                        rows_col="rows_count",
                    )
            else:
                if monthly.empty:
                    st.markdown('<div class="empty">No monthly rows in selected range. Showing daily fallback.</div>', unsafe_allow_html=True)
                    pulse_df = daily[["service_date", "rows_count", "total_passengers", "total_vehicles"]].copy()
                    st.vega_lite_chart(_trend_spec(daily), width="stretch")
                    verify = _timeseries_verify(
                        daily,
                        date_col="service_date",
                        passengers_col="total_passengers",
                        vehicles_col="total_vehicles",
                        rows_col="rows_count",
                    )
                else:
                    monthly_pulse = monthly.rename(columns={"year_month": "service_date"})[
                        ["service_date", "rows_count", "total_passengers", "total_vehicles"]
                    ].copy()
                    pulse_df = monthly_pulse
                    st.vega_lite_chart(_trend_spec(monthly_pulse), width="stretch")
                    verify = _timeseries_verify(
                        monthly_pulse,
                        date_col="service_date",
                        passengers_col="total_passengers",
                        vehicles_col="total_vehicles",
                        rows_col="rows_count",
                    )

            with st.expander("Verify traffic pulse numbers"):
                st.caption(
                    f"Source: {selected_grain} grain | "
                    f"{verify['start']} -> {verify['end']} | "
                    f"Points: {int(verify['points'])} | "
                    f"Total passengers: {float(verify['passengers']):,.0f} | "
                    f"Total vehicles: {float(verify['vehicles']):,.0f} | "
                    f"Total sailings: {float(verify['sailings']):,.0f}"
                )
                st.dataframe(
                    pulse_df.rename(columns={
                        "service_date": "Period",
                        "rows_count": "Sailings",
                        "total_passengers": "Passengers",
                        "total_vehicles": "Vehicles",
                    }),
                    hide_index=True,
                    width="stretch",
                )

            # _render_deepen_insight("traffic_pulse", deepen_traffic_pulse_snapshot)

        with st.container(border=True):
            _section_header("Top Corridors", "Highest passenger corridors in the selected View Window.")
            if route_rank.empty:
                st.markdown('<div class="empty">No route rows for this range.</div>', unsafe_allow_html=True)
            else:
                top_routes = route_rank.head(top_n)
                st.vega_lite_chart(_bar_spec(top_routes, "pair_label", "passengers", CHART_ROUTE_BAR_COLOR), width="stretch")
                with st.expander("Verify top corridors numbers"):
                    st.dataframe(
                        top_routes[["pair_label", "passengers", "vehicles", "sailings"]].rename(
                            columns={
                                "pair_label": "Corridor",
                                "passengers": "Passengers",
                                "vehicles": "Vehicles",
                                "sailings": "Sailings",
                            }
                        ),
                        hide_index=True,
                        width="stretch",
                    )

            # _render_deepen_insight("top_corridors", deepen_top_corridors_snapshot)

    with right:
        with st.container():
            freshness_label = "Using live deterministic report for the current view."
            if use_cached_auto_panel:
                freshness_label = _format_snapshot_age(agent_snapshot_freshness.hours_since_latest)
            report_period_label = f"Report period: {current_period_text}"
            if use_cached_auto_panel and agent_snapshot is not None and agent_snapshot.kpi_start and agent_snapshot.kpi_end:
                report_period_label = (
                    f"Report period: {pd.to_datetime(agent_snapshot.kpi_start).strftime('%d %b %Y')} "
                    f"-> {pd.to_datetime(agent_snapshot.kpi_end).strftime('%d %b %Y')}"
                )

            st.markdown(
                f"""
                <div class="agent-wrap">
                    <div class="agent">
                        <div class="agent-head">
                            <div class="agent-icon">⚓</div>
                            <div>
                                <h4>{escape(agent_card_title)}</h4>
                                <small>{escape(agent_subtitle)}</small>
                            </div>
                        </div>
                        <div class="agent-flags">
                            <span class="agent-flag">{escape(freshness_label)}</span>
                            <span class="agent-flag">{escape(report_period_label)}</span>
                        </div>
                        <div class="agent-report">
                            {agent_report_html}
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if use_cached_auto_panel and agent_snapshot_freshness.is_stale:
                st.warning("Intelligence snapshots are stale or missing (no recent auto_panel snapshot in last 48 hours).")
        with st.container(border=True):
            _section_header("Port Balance", "Departure + arrival passenger totals by port in the selected View Window.")
            if port_rank.empty:
                st.markdown('<div class="empty">No port rows for this range.</div>', unsafe_allow_html=True)
            else:
                ports_plot = port_rank.head(top_n).copy()
                ports_plot["total"] = ports_plot["total_departing_passengers"] + ports_plot["total_arriving_passengers"]
                st.vega_lite_chart(_bar_spec(ports_plot, "port_name", "total", CHART_PORT_BAR_COLOR), width="stretch")
                with st.expander("Verify port balance numbers"):
                    st.dataframe(
                        ports_plot[
                            [
                                "port_name",
                                "total_departing_passengers",
                                "total_arriving_passengers",
                                "total",
                            ]
                        ].rename(
                            columns={
                                "port_name": "Port",
                                "total_departing_passengers": "Departing pax",
                                "total_arriving_passengers": "Arriving pax",
                                "total": "Total pax",
                            }
                        ),
                        hide_index=True,
                        width="stretch",
                    )

            # _render_deepen_insight("port_balance", deepen_port_balance_snapshot)

else:
    top_corridor_label = route_rank.iloc[0]["pair_label"] if not route_rank.empty else "No data"
    top_corridor_passengers = float(route_rank.iloc[0]["passengers"]) if not route_rank.empty else 0.0
    net_leader = "No data"
    net_value = 0.0
    if not top_ports_net.empty:
        net_df = top_ports_net.copy()
        net_df["net"] = net_df["total_arriving_passengers"] - net_df["total_departing_passengers"]
        row = net_df.iloc[net_df["net"].abs().argmax()]
        net_leader = str(row["port_name"])
        net_value = float(row["net"])
    net_direction = "arrival-heavy" if net_value >= 0 else "departure-heavy"
    net_sign = "+" if net_value >= 0 else "-"
    net_color = CHART_NETFLOW_POS_COLOR if net_value >= 0 else CHART_NETFLOW_NEG_COLOR
    net_value_compact = f"{net_sign}{_fmt_compact(abs(net_value))}"

    st.markdown(
        f"""
        <div class="topbar">
            <div>
                <h1 class="title">Analytics Studio</h1>
                <p class="subtitle">Deeper behavior diagnostics for {selected_start.strftime('%d %b %Y')} -> {selected_end.strftime('%d %b %Y')} ({selected_grain} grain, {escape(selected_route)}, {escape(selected_port)}).</p>
            </div>
        </div>
        <div class="analytics-chips">
            <div class="analytics-chip"><div class="k">Dominant Corridor</div><div class="v">{escape(top_corridor_label)}</div></div>
            <div class="analytics-chip"><div class="k">Top Corridor Pax</div><div class="v">{_fmt_compact(top_corridor_passengers)}</div></div>
            <div class="analytics-chip"><div class="k">Largest Port Imbalance</div><div class="v">{escape(net_leader)} (<span style="color:{net_color};">{net_value_compact}</span> {escape(net_direction)})</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    row1_left, row1_right = st.columns((1.9, 1), gap="large")
    with row1_left:
        with st.container(border=True):
            _section_header("Demand and Concentration", f"{selected_grain.title()} passenger volume with the Top-3 corridor concentration signal.")
            if corridor_focus.empty:
                st.markdown('<div class="empty">No concentration trend available for this range.</div>', unsafe_allow_html=True)
            else:
                st.vega_lite_chart(_focus_concentration_spec(corridor_focus, grain=selected_grain), width="stretch")

    with row1_right:
        with st.container(border=True):
            _section_header("Weekday x Month Matrix", "Average passenger intensity by weekday across each month.")
            st.vega_lite_chart(_weekday_heatmap_spec(daily), width="stretch")

    row2_left, row2_right = st.columns((1.25, 1), gap="large")
    with row2_left:
        with st.container(border=True):
            _section_header("Corridor Efficiency Map", "Each point is a corridor sized by sailings and colored by share. Click to select, double-click to reset.")
            if top_routes_scatter.empty:
                st.markdown('<div class="empty">No route rows for this range.</div>', unsafe_allow_html=True)
            else:
                st.vega_lite_chart(_corridor_efficiency_spec(top_routes_scatter, route_scope_total_passengers), width="stretch")

    with row2_right:
        with st.container(border=True):
            _section_header("Port Net Flow", "Net passenger flow by port. Positive means arrival-heavy.")
            if top_ports_net.empty:
                st.markdown('<div class="empty">No port rows for this range.</div>', unsafe_allow_html=True)
            else:
                # if active_page == "analytics":
                #     st.caption(
                #         f"Scoped by filters: {selected_start.strftime('%d %b %Y')} -> {selected_end.strftime('%d %b %Y')} | "
                #         f"{selected_route} | {selected_port} | top {top_n}"
                #     )
                st.vega_lite_chart(_port_netflow_spec(top_ports_net), width="stretch")

                if active_page == "analytics":
                    with st.expander("Verify netflow numbers (routes vs ports aggregation)"):
                        reconcile = _port_netflow_reconciliation(routes, ports)
                        max_abs_net_diff = float(reconcile["abs_net_diff"].max()) if not reconcile.empty else 0.0
                        st.caption(
                            f"Max absolute net-flow difference across ports: {max_abs_net_diff:,.0f}. "
                            "Expected result: 0 when both sources are consistent."
                        )
                        st.dataframe(
                            reconcile[
                                [
                                    "port_name",
                                    "dep_routes",
                                    "dep_ports",
                                    "arr_routes",
                                    "arr_ports",
                                    "net_routes",
                                    "net_ports",
                                    "net_diff",
                                ]
                            ],
                            hide_index=True,
                            width="stretch",
                        )
                        current_top_ports = top_ports_net["port_name"].tolist()
                        st.caption(
                            "Top ports (ranked by absolute net flow): "
                            + (", ".join(current_top_ports) if current_top_ports else "None")
                        )

    row3_left, row3_right = st.columns((1.15, 1), gap="large")
    with row3_left:
        with st.container(border=True):
            weekday_source = _daily_from_routes(routes) if active_page == "analytics" else daily
            scoped_days = int(weekday_source["service_date"].nunique()) if not weekday_source.empty else 0
            scoped_pax = float(weekday_source["total_passengers"].sum()) if not weekday_source.empty else 0.0
            weekday_chart_key = (
                "weekday_profile_"
                f"{selected_start.isoformat()}_{selected_end.isoformat()}_"
                f"{selected_route}_{selected_port}_{scoped_days}_{int(scoped_pax)}"
            )
            weekday_verify_df, weekday_verify = _weekday_profile_verify(weekday_source)
            weekday_chart_df = weekday_verify_df.copy()
            weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            weekday_chart_df["weekday"] = pd.Categorical(weekday_chart_df["weekday"], categories=weekday_order, ordered=True)
            weekday_chart = (
                alt.Chart(weekday_chart_df)

                .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5, color=CHART_PRIMARY_BAR_COLOR)
                .encode(
                    x=alt.X("weekday:N", sort=weekday_order, title=None),
                    y=alt.Y("avg_passengers:Q", title="Avg passengers"),
                    tooltip=[
                        alt.Tooltip("weekday:N", title="Weekday"),
                        alt.Tooltip("avg_passengers:Q", title="Avg passengers", format=",.0f"),
                        alt.Tooltip("day_count:Q", title="Days in scope", format=",.0f"),
                        alt.Tooltip("total_passengers:Q", title="Total passengers", format=",.0f"),
                    ],
                )
                .properties(height=260)
                .configure(background="transparent")
                .configure_view(stroke=None)
                .configure_axis(
                    labelColor=CHART_AXIS["labelColor"],
                    titleColor=CHART_AXIS["titleColor"],
                    gridColor=CHART_AXIS["gridColor"],
                    domainColor=CHART_AXIS["domainColor"],
                    tickColor=CHART_AXIS["tickColor"],
                    labelFontSize=11,
                    titleFontSize=11,
                    gridDash=[3, 3],
                )
            )
            _section_header("Weekday Demand Profile", "Average passenger demand by day of week.")
            st.altair_chart(weekday_chart, width='stretch', key=weekday_chart_key)
            st.caption(
                "Avg passengers by weekday | "
                f"Scope: {selected_start.isoformat()} to {selected_end.isoformat()}, "
                f"Route: {selected_route}, Port: {selected_port} | "
                f"Computed at daily grain | Days: {scoped_days}, Total pax: {scoped_pax:,.0f}"
            )
            with st.expander("Verify weekday profile numbers"):
                weekday_verify_df, weekday_verify = _weekday_profile_verify(weekday_source)
                st.caption(
                    "Chart source window: "
                    f"{weekday_verify['start']} -> {weekday_verify['end']} | "
                    f"Days: {int(weekday_verify['days'])} | "
                    f"Total pax: {float(weekday_verify['total_pax']):,.0f}"
                )
                st.caption(
                    "Sunday avg passengers (current scoped source): "
                    f"{float(weekday_verify['sunday_avg']):,.0f}"
                )
                st.dataframe(
                    weekday_verify_df.rename(
                        columns={
                            "weekday": "Weekday",
                            "avg_passengers": "Avg passengers",
                            "day_count": "Days in scope",
                            "total_passengers": "Total passengers",
                        }
                    ),
                    hide_index=True,
                    width="stretch",
                )

    with row3_right:
        with st.container(border=True):
            _section_header("Corridor Detail", "Operational intensity by ranked corridor.")
            if route_rank.empty:
                st.markdown('<div class="empty">No corridor table rows for this range.</div>', unsafe_allow_html=True)
            else:
                detail = route_rank.head(top_n).copy()
                detail["passengers_per_sailing"] = (detail["passengers"] / detail["sailings"].replace(0, pd.NA)).fillna(0)
                detail["vehicles_per_sailing"] = (detail["vehicles"] / detail["sailings"].replace(0, pd.NA)).fillna(0)
                detail["share_pct"] = (
                    (detail["passengers"] / route_scope_total_passengers) * 100
                ).fillna(0) if route_scope_total_passengers > 0 else 0.0
                st.dataframe(
                    detail[["pair_label", "share_pct", "passengers_per_sailing", "vehicles_per_sailing", "sailings"]].rename(
                        columns={
                            "pair_label": "Corridor",
                            "share_pct": "Share %",
                            "passengers_per_sailing": "Passengers / Sailing",
                            "vehicles_per_sailing": "Vehicles / Sailing",
                            "sailings": "Sailings",
                        }
                    ),
                    hide_index=True,
                    width="stretch",
                )


