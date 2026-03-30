"""@bruin
name: thalassa.auto_panel_snapshot_writer
type: python
image: python:3.11
connection: gcp-default
secrets:
  - key: gcp-default
    inject_as: GCP_CONN
  - key: openrouter_api_key
    inject_as: OPENROUTER_API_KEY
  - key: openrouter_model
    inject_as: OPENROUTER_MODEL
  - key: openrouter_base_url
    inject_as: OPENROUTER_BASE_URL

depends:
  - thalassa.intelligence_snapshots
@bruin"""

from __future__ import annotations

import json
import os
import runpy
from pathlib import Path


def _bootstrap_env_from_connection_secret() -> None:
    """Populate runtime env vars from injected Bruin connection JSON when available."""
    raw = os.getenv("GCP_CONN", "").strip()
    if not raw:
        return

    try:
        conn = json.loads(raw)
    except Exception as exc:
        print(f"Warning: failed to parse GCP_CONN secret: {exc}")
        return

    project_id = str(conn.get("project_id", "")).strip()
    if not project_id and isinstance(conn.get("details"), dict):
        project_id = str(conn["details"].get("project_id", "")).strip()

    service_account_json = str(conn.get("service_account_json", "")).strip()
    if not service_account_json and isinstance(conn.get("details"), dict):
        service_account_json = str(conn["details"].get("service_account_json", "")).strip()

    if project_id:
        os.environ.setdefault("THALASSA_BQ_PROJECT", project_id)
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)
        os.environ.setdefault("THALASSA_BQ_QUOTA_PROJECT", project_id)

    if service_account_json:
        os.environ.setdefault("THALASSA_GCP_SERVICE_ACCOUNT_JSON", service_account_json)


def main() -> None:
    _bootstrap_env_from_connection_secret()
    project_root = Path(__file__).resolve().parents[3]
    script_path = project_root / "scripts" / "generate_auto_panel_snapshot.py"
    try:
        runpy.run_path(str(script_path), run_name="__main__")
    except ValueError as exc:
        message = str(exc)
        if "Unable to resolve BigQuery project" in message or "Set THALASSA_BQ_PROJECT" in message:
            print(f"Skipping auto panel snapshot writer: {message}")
            return
        raise


if __name__ == "__main__":
    main()
