"""@bruin
name: thalassa.raw_sailing_traffic
type: python
image: python:3.11
connection: gcp-default

materialization:
  type: table
  strategy: append

columns:
  - name: record_hash
    type: string
    primary_key: true
    checks:
      - name: not_null
  - name: service_date
    type: date
  - name: route_code
    type: string
  - name: route_name
    type: string
  - name: departure_port
    type: string
  - name: arrival_port
    type: string
  - name: passenger_count
    type: integer
  - name: vehicle_count
    type: integer
  - name: ingested_at
    type: timestamp
  - name: raw_payload
    type: string
@bruin"""

import hashlib
import json
import os
import random
import time
from datetime import UTC, datetime

import pandas as pd
import requests


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))

    window_unit = str(vars.get("request_window_unit", "day")).strip().lower()
    window_size = int(vars.get("request_window_size", 1))
    max_retries = int(vars.get("request_max_retries", 3))
    retry_base_delay_seconds = float(vars.get("request_retry_base_delay_seconds", 1.0))
    retry_max_delay_seconds = float(vars.get("request_retry_max_delay_seconds", 30.0))
    failed_window_replay_passes = int(vars.get("request_failed_window_replay_passes", 2))
    failed_window_replay_delay_seconds = float(vars.get("request_failed_window_replay_delay_seconds", 5.0))
    source_data_lag_days = int(vars.get("source_data_lag_days", 0))

    if window_unit not in {"day", "month", "year"}:
        raise ValueError("request_window_unit must be one of: day, month, year")
    if window_size < 1:
        raise ValueError("request_window_size must be >= 1")
    if max_retries < 0:
        raise ValueError("request_max_retries must be >= 0")
    if retry_base_delay_seconds <= 0:
        raise ValueError("request_retry_base_delay_seconds must be > 0")
    if retry_max_delay_seconds <= 0:
        raise ValueError("request_retry_max_delay_seconds must be > 0")
    if failed_window_replay_passes < 0:
        raise ValueError("request_failed_window_replay_passes must be >= 0")
    if failed_window_replay_delay_seconds < 0:
        raise ValueError("request_failed_window_replay_delay_seconds must be >= 0")
    if source_data_lag_days < 0:
        raise ValueError("source_data_lag_days must be >= 0")

    class NonRetryableRequestError(RuntimeError):
        pass

    retryable_status_codes = {429, 500, 502, 503, 504}

    def fetch_window_payload(window_start_str: str, window_end_str: str):
        total_attempts = max_retries + 1
        for attempt in range(1, total_attempts + 1):
            try:
                response = requests.get(
                    "https://data.gov.gr/api/v1/query/sailing_traffic",
                    params={"date_from": window_start_str, "date_to": window_end_str},
                    timeout=60,
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in retryable_status_codes:
                    raise NonRetryableRequestError(
                        f"Non-retryable HTTP {status_code} for {window_start_str} -> {window_end_str}: {exc}"
                    ) from exc
                if attempt == total_attempts:
                    raise
            except Exception:
                if attempt == total_attempts:
                    raise

                exponential_delay = retry_base_delay_seconds * (2 ** (attempt - 1))
                capped_delay = min(exponential_delay, retry_max_delay_seconds)
                jittered_delay = capped_delay * random.uniform(0.5, 1.5)
                sleep_seconds = min(jittered_delay, retry_max_delay_seconds)
                print(
                    f"Retrying {window_start_str} -> {window_end_str} "
                    f"(attempt {attempt + 1}/{total_attempts}) after {sleep_seconds:.2f}s"
                )
                time.sleep(sleep_seconds)

    def extract_records(payload: object, window_start_str: str, window_end_str: str) -> list[dict]:
        if isinstance(payload, list):
            return [record for record in payload if isinstance(record, dict)]
        if isinstance(payload, dict):
            records = None
            for key in ("records", "results", "data"):
                if key in payload:
                    records = payload[key]
                    break

            if records is None:
                raise ValueError(
                    f"Unexpected response shape for {window_start_str} -> {window_end_str}: "
                    f"expected one of records/results/data keys, got {sorted(payload.keys())}"
                )
            if not isinstance(records, list):
                raise ValueError(
                    f"Unexpected response payload for {window_start_str} -> {window_end_str}: "
                    f"expected {key} to be a list, got {type(records).__name__}"
                )

            return [record for record in records if isinstance(record, dict)]

        raise ValueError(
            f"Unexpected response payload for {window_start_str} -> {window_end_str}: "
            f"expected list or dict, got {type(payload).__name__}"
        )

    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()

    if source_data_lag_days:
        lag_delta = pd.Timedelta(days=source_data_lag_days)
        start = start - lag_delta
        end = end - lag_delta
        print(
            "Applying source lag of "
            f"{source_data_lag_days} day(s): scheduled {start_date} -> {end_date}, "
            f"effective {start.strftime('%Y-%m-%d')} -> {end.strftime('%Y-%m-%d')}"
        )

    if start > end:
        raise ValueError(f"BRUIN_START_DATE ({start_date}) must be <= BRUIN_END_DATE ({end_date})")

    rows = []
    failed_windows: list[tuple[str, str, str]] = []
    empty_windows = 0
    total_windows = 0
    ingested_at = datetime.now(UTC).isoformat()
    current = start

    def append_records(records: list[dict]):
        for record in records:
            raw_payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
            rows.append(
                {
                    "record_hash": hashlib.sha256(raw_payload.encode("utf-8")).hexdigest(),
                    "service_date": record.get("date"),
                    "route_code": record.get("routecode"),
                    "route_name": record.get("routecodenames"),
                    "departure_port": record.get("departureportname"),
                    "arrival_port": record.get("arrivalportname"),
                    "passenger_count": record.get("passengercount"),
                    "vehicle_count": record.get("vehiclecount"),
                    "ingested_at": ingested_at,
                    "raw_payload": raw_payload,
                }
            )

    while current <= end:
        total_windows += 1
        if window_unit == "day":
            next_start = current + pd.DateOffset(days=window_size)
        elif window_unit == "month":
            next_start = current + pd.DateOffset(months=window_size)
        else:
            next_start = current + pd.DateOffset(years=window_size)

        window_end = min(next_start - pd.Timedelta(days=1), end)
        window_start_str = current.strftime("%Y-%m-%d")
        window_end_str = window_end.strftime("%Y-%m-%d")

        try:
            payload = fetch_window_payload(window_start_str, window_end_str)
        except NonRetryableRequestError:
            raise
        except Exception as exc:
            print(f"Failed to fetch {window_start_str} -> {window_end_str}: {exc}")
            failed_windows.append((window_start_str, window_end_str, f"{type(exc).__name__}: {exc}"))
            current = next_start
            continue

        records = extract_records(payload, window_start_str, window_end_str)

        if not records:
            print(f"No records returned for {window_start_str} -> {window_end_str}")
            empty_windows += 1
            current = next_start
            continue

        append_records(records)
        print(f"Loaded {len(records)} rows for {window_start_str} -> {window_end_str}")
        current = next_start

    if failed_windows and failed_window_replay_passes > 0:
        for replay_pass in range(1, failed_window_replay_passes + 1):
            pending_windows = failed_windows
            failed_windows = []
            print(
                f"Replaying {len(pending_windows)} failed windows "
                f"(pass {replay_pass}/{failed_window_replay_passes})"
            )

            for window_start_str, window_end_str, _ in pending_windows:
                try:
                    payload = fetch_window_payload(window_start_str, window_end_str)
                except NonRetryableRequestError:
                    raise
                except Exception as exc:
                    print(f"Replay failed for {window_start_str} -> {window_end_str}: {exc}")
                    failed_windows.append((window_start_str, window_end_str, f"{type(exc).__name__}: {exc}"))
                    continue

                records = extract_records(payload, window_start_str, window_end_str)
                if not records:
                    print(f"No records returned for {window_start_str} -> {window_end_str} on replay")
                    empty_windows += 1
                    continue

                append_records(records)
                print(f"Loaded {len(records)} rows for {window_start_str} -> {window_end_str} on replay")

            if not failed_windows:
                break

            if replay_pass < failed_window_replay_passes and failed_window_replay_delay_seconds > 0:
                replay_delay_seconds = min(
                    failed_window_replay_delay_seconds * replay_pass,
                    retry_max_delay_seconds * 4,
                )
                print(
                    f"Waiting {replay_delay_seconds:.2f}s before next replay pass "
                    f"for {len(failed_windows)} windows"
                )
                time.sleep(replay_delay_seconds)

    if failed_windows:
        sample_failures = failed_windows[:5]
        remaining_failures = len(failed_windows) - len(sample_failures)
        failure_summary = "; ".join(
            f"{window_start} -> {window_end}: {message}"
            for window_start, window_end, message in sample_failures
        )
        if remaining_failures > 0:
            failure_summary = f"{failure_summary}; ... and {remaining_failures} more"
        raise RuntimeError(
            f"Failed to fetch {len(failed_windows)} of {total_windows} windows. "
            f"Empty windows: {empty_windows}. Failures: {failure_summary}"
        )

    print(
        f"Completed {total_windows} windows for {start.strftime('%Y-%m-%d')} -> {end.strftime('%Y-%m-%d')} "
        f"(scheduled {start_date} -> {end_date}). "
        f"Loaded {len(rows)} rows across {total_windows - empty_windows} non-empty windows. "
        f"Empty windows: {empty_windows}."
    )

    if not rows:
        return pd.DataFrame(
            columns=[
                "record_hash",
                "service_date",
                "route_code",
                "route_name",
                "departure_port",
                "arrival_port",
                "passenger_count",
                "vehicle_count",
                "ingested_at",
                "raw_payload",
            ]
        )

    return pd.DataFrame(rows)
