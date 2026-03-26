from __future__ import annotations

from google.cloud import bigquery

from .models import SnapshotPayload


def upsert_snapshot(
    client: bigquery.Client,
    *,
    table_fqn: str,
    payload: SnapshotPayload,
) -> None:
    query = f"""
    MERGE `{table_fqn}` AS target
    USING (
      SELECT
        @snapshot_ts AS snapshot_ts,
        @chart_id AS chart_id,
        @insight_type AS insight_type,
        @grain AS grain,
        @filter_hash AS filter_hash,
        @data_version AS data_version,
        @title AS title,
        @summary AS summary,
        @facts AS facts,
        @anomalies AS anomalies,
        @comparisons AS comparisons,
        @recommendations AS recommendations,
        @report_text AS report_text,
        @insight_text AS insight_text,
        @generation_mode AS generation_mode,
        @generation_error AS generation_error,
        @model_name AS model_name
    ) AS src
    ON target.chart_id = src.chart_id
       AND target.insight_type = src.insight_type
       AND target.data_version = src.data_version
       AND IFNULL(target.filter_hash, '') = IFNULL(src.filter_hash, '')
    WHEN MATCHED THEN UPDATE SET
       snapshot_ts = src.snapshot_ts,
       grain = src.grain,
       title = src.title,
       summary = src.summary,
       facts = src.facts,
       anomalies = src.anomalies,
       comparisons = src.comparisons,
       recommendations = src.recommendations,
       report_text = src.report_text,
       insight_text = src.insight_text,
       generation_mode = src.generation_mode,
       generation_error = src.generation_error,
       model_name = src.model_name
    WHEN NOT MATCHED THEN
      INSERT (
        snapshot_ts,
        chart_id,
        insight_type,
        grain,
        filter_hash,
        data_version,
        title,
        summary,
        facts,
        anomalies,
        comparisons,
        recommendations,
        report_text,
        insight_text,
        generation_mode,
        generation_error,
        model_name
      )
      VALUES (
        src.snapshot_ts,
        src.chart_id,
        src.insight_type,
        src.grain,
        src.filter_hash,
        src.data_version,
        src.title,
        src.summary,
        src.facts,
        src.anomalies,
        src.comparisons,
        src.recommendations,
        src.report_text,
        src.insight_text,
        src.generation_mode,
        src.generation_error,
        src.model_name
      )
    """

    params = [
        bigquery.ScalarQueryParameter("snapshot_ts", "TIMESTAMP", payload.snapshot_ts_iso),
        bigquery.ScalarQueryParameter("chart_id", "STRING", payload.chart_id),
        bigquery.ScalarQueryParameter("insight_type", "STRING", payload.insight_type),
        bigquery.ScalarQueryParameter("grain", "STRING", payload.grain),
        bigquery.ScalarQueryParameter("filter_hash", "STRING", payload.filter_hash),
        bigquery.ScalarQueryParameter("data_version", "STRING", payload.data_version),
        bigquery.ScalarQueryParameter("title", "STRING", payload.output.title),
        bigquery.ScalarQueryParameter("summary", "STRING", payload.output.summary),
        bigquery.ArrayQueryParameter("facts", "STRING", payload.output.facts),
        bigquery.ArrayQueryParameter("anomalies", "STRING", payload.output.anomalies),
        bigquery.ArrayQueryParameter("comparisons", "STRING", payload.output.comparisons),
        bigquery.ArrayQueryParameter("recommendations", "STRING", payload.output.recommendations),
        bigquery.ScalarQueryParameter("report_text", "STRING", payload.output.report_text),
        bigquery.ScalarQueryParameter("insight_text", "STRING", payload.output.insight_text),
        bigquery.ScalarQueryParameter("generation_mode", "STRING", payload.meta.generation_mode),
        bigquery.ScalarQueryParameter("generation_error", "STRING", payload.meta.generation_error),
        bigquery.ScalarQueryParameter("model_name", "STRING", payload.meta.model_name),
    ]

    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()
