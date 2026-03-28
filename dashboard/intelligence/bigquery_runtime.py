"""Shared BigQuery auth and dataframe helpers for dashboard and scripts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

import google.auth
import pandas as pd
from google.auth.credentials import Credentials
from google.cloud import bigquery
from google.oauth2 import credentials as user_credentials
from google.oauth2 import service_account

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _env_flag(name: str, default: bool = False) -> bool:
    """Read a boolean flag from the environment."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _load_service_account_info(
    *,
    streamlit_secrets: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Load service-account credentials from Streamlit secrets or environment."""
    if streamlit_secrets:
        try:
            if "gcp_service_account" in streamlit_secrets:
                value = streamlit_secrets["gcp_service_account"]
                if isinstance(value, Mapping):
                    return dict(value)
        except Exception:
            pass

    service_account_json = os.getenv("THALASSA_GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if service_account_json:
        return json.loads(service_account_json)

    service_account_file = (
        os.getenv("THALASSA_GCP_SERVICE_ACCOUNT_FILE", "").strip()
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    )
    if service_account_file:
        return json.loads(Path(service_account_file).read_text(encoding="utf-8"))

    return None


def _with_quota_project(credentials: Credentials, quota_project_id: str | None) -> Credentials:
    """Attach a quota project when the credential type supports it."""
    if not quota_project_id or not hasattr(credentials, "with_quota_project"):
        return credentials

    try:
        return credentials.with_quota_project(quota_project_id)
    except Exception:
        return credentials


def resolve_bigquery_credentials(
    project: str,
    *,
    streamlit_secrets: Mapping[str, Any] | None = None,
) -> Credentials:
    """Resolve BigQuery credentials with explicit quota-project handling."""
    quota_project = os.getenv("THALASSA_BQ_QUOTA_PROJECT", "").strip() or project
    service_account_info = _load_service_account_info(streamlit_secrets=streamlit_secrets)

    if service_account_info:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=[CLOUD_PLATFORM_SCOPE],
        )
        return _with_quota_project(credentials, quota_project)

    credentials, _ = google.auth.default(
        scopes=[CLOUD_PLATFORM_SCOPE],
        quota_project_id=quota_project,
    )
    credentials = _with_quota_project(credentials, quota_project)

    if isinstance(credentials, user_credentials.Credentials):
        if _env_flag("THALASSA_BQ_REQUIRE_SERVICE_ACCOUNT", default=False):
            raise RuntimeError(
                "End-user ADC credentials are not allowed when "
                "THALASSA_BQ_REQUIRE_SERVICE_ACCOUNT=true. Configure "
                "st.secrets['gcp_service_account'], THALASSA_GCP_SERVICE_ACCOUNT_JSON, "
                "THALASSA_GCP_SERVICE_ACCOUNT_FILE, or attach a service account to the runtime."
            )

        if not getattr(credentials, "quota_project_id", None):
            raise RuntimeError(
                "Application Default Credentials are using end-user credentials without a "
                f"quota project. Run `gcloud auth application-default set-quota-project {project}` "
                "or set THALASSA_BQ_QUOTA_PROJECT."
            )

    return credentials


def create_bigquery_client(
    project: str,
    location: str | None,
    *,
    streamlit_secrets: Mapping[str, Any] | None = None,
) -> bigquery.Client:
    """Create a BigQuery client with explicit credentials and quota project."""
    credentials = resolve_bigquery_credentials(project, streamlit_secrets=streamlit_secrets)
    return bigquery.Client(project=project, credentials=credentials, location=location)


def should_use_bigquery_storage() -> bool:
    """Whether to use the Storage API for dataframe fetches."""
    if not _env_flag("THALASSA_BQ_USE_STORAGE_API", default=False):
        return False

    try:
        from google.cloud import bigquery_storage  # noqa: F401
    except ImportError:
        return False

    return True


def job_to_dataframe(job: bigquery.job.QueryJob) -> pd.DataFrame:
    """Convert a query job to a dataframe with Storage API kept opt-in."""
    return job.to_dataframe(create_bqstorage_client=should_use_bigquery_storage())
