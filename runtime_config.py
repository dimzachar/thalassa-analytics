"""Shared runtime configuration helpers for warehouse settings."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_BQ_DATASET = "thalassa"
DEFAULT_INTELLIGENCE_TABLE = "intelligence_snapshots"


def load_env_file(path: Path) -> None:
    """Load a minimal .env file without overriding existing environment values."""
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue

        key, value = raw_line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key.removeprefix("export ").strip()
        if not key or key in os.environ:
            continue

        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


load_env_file(PROJECT_ROOT / ".env")


def get_bq_project() -> str:
    """Return the configured BigQuery project ID."""
    return os.getenv("THALASSA_BQ_PROJECT", "").strip()


def get_bq_dataset() -> str:
    """Return the configured BigQuery dataset ID."""
    return os.getenv("THALASSA_BQ_DATASET", DEFAULT_BQ_DATASET).strip() or DEFAULT_BQ_DATASET


def get_bq_location() -> str | None:
    """Return the configured BigQuery location, when present."""
    value = os.getenv("THALASSA_BQ_LOCATION", "").strip()
    return value or None


def get_intelligence_table(dataset: str | None = None) -> str:
    """Return the intelligence snapshot table for the active dataset."""
    dataset_id = dataset or get_bq_dataset()
    return f"{dataset_id}.{DEFAULT_INTELLIGENCE_TABLE}"


def build_dataset_table(table_name: str, dataset: str | None = None) -> str:
    """Attach the active dataset to a relative table name."""
    if "." in table_name:
        return table_name
    dataset_id = dataset or get_bq_dataset()
    return f"{dataset_id}.{table_name}"


def qualify_bigquery_table(project: str, dataset: str, table_name: str) -> str:
    """Return a full BigQuery table reference without backticks."""
    parts = table_name.split(".")
    if len(parts) == 3:
        return table_name
    if len(parts) == 2:
        return f"{project}.{parts[0]}.{parts[1]}"
    return f"{project}.{dataset}.{table_name}"
