# Agent Panel (Maritime Intelligence)

The agent panel is the narrative card on the dashboard that summarizes recent traffic activity. It is designed to be reliable by default and optionally enhanced with an LLM overlay when notable changes are detected.

## What We Built

- A cached snapshot pipeline that writes narrative content into BigQuery.
- A deterministic fallback report that always renders, even without LLM keys.
- A gating rule that only calls the LLM when the data suggests a meaningful change.
- A dashboard integration that prefers cached snapshots for the default view and falls back to live deterministic text for filtered views.

## Data Sources

The snapshot builder pulls data from the curated daily tables:

- `row_counts_daily` for overall activity trends
- `fct_route_traffic_daily` for corridor rankings
- `fct_port_activity_daily` for port balance

## Snapshot Generation Flow

1. `scripts/generate_auto_panel_snapshot.py` loads the daily, route, and port tables for a lookback window.
2. `dashboard/intelligence/agents/detector_agent.py` decides whether the change is notable enough to justify an LLM overlay.
3. `dashboard/intelligence/agents/snapshot_agent.py` builds a structured source snapshot used by both deterministic and LLM paths.
4. `dashboard/intelligence/agents/generation_agent.py` either renders a deterministic report or requests an LLM overlay and validates it against the snapshot.
5. `dashboard/intelligence/repositories.py` upserts the snapshot into BigQuery.

The pipeline entrypoint is `pipeline/assets/reports/auto_panel_snapshot_writer.py`, which runs the same script during Bruin executions.

## Defaults And Overrides

Defaults used by the generator:

- Lookback window: 90 days
- KPI window: 7 days
- Snapshot end date: latest available `service_date`

Optional environment overrides:

- `THALASSA_SNAPSHOT_END_DATE` to cap the snapshot end date
- `THALASSA_SNAPSHOT_LOOKBACK_DAYS` to change the lookback window
- `THALASSA_KPI_WINDOW_DAYS` to change the KPI window used for the card
- `THALASSA_SNAPSHOT_PRESETS` for comma-separated presets like `year,90` or `full`

## LLM Behavior And Safety

LLM usage is optional and guarded:

- If no notable change is detected, the panel uses a deterministic report.
- If LLM keys are missing, the panel uses a deterministic report and sets the mode to `deterministic_no_api_key`.
- If the LLM fails, the panel uses a deterministic report and sets the mode to `deterministic_llm_error`.

Provider configuration lives in `dashboard/intelligence/config.py` and `.env.example`:

- `LLM_PROVIDER` can be `openrouter`, `anthropic`, or `gemini`.
- Set the matching API key and model variables for the provider you use.

## Dashboard Rendering Rules

The dashboard uses cached snapshots only when all of the following are true:

- The active page is the main dashboard.
- Filters are in the default state (`All routes`, `All ports`).
- The snapshot KPI window matches the currently selected date range.

If any condition fails, `dashboard/intelligence/agents/reporting_agent.py` builds a live deterministic summary for the current filters.

Snapshot freshness is checked in `dashboard/intelligence/repositories.py`, and the UI warns if no auto panel snapshot is available in the last 48 hours.

## How To Refresh The Panel

To regenerate snapshots directly:

```bash
uv run python ./scripts/generate_auto_panel_snapshot.py
```

To run through Bruin:

```bash
bruin run ./pipeline/assets/reports/auto_panel_snapshot_writer.py --downstream
```

The SQL asset `pipeline/assets/reports/intelligence_snapshots.sql` seeds other snapshot rows and ensures the table exists. The auto panel snapshot is written by the Python writer asset above.
