/* @bruin
name: thalassa.intelligence_snapshots
type: bq.sql

depends:
  - thalassa.row_counts_daily
  - thalassa.fct_route_traffic_daily
  - thalassa.fct_port_activity_daily

materialization:
  type: table
@bruin */

WITH scope AS (
    SELECT
        MAX(service_date) AS end_date,
        GREATEST(DATE_SUB(MAX(service_date), INTERVAL 89 DAY), MIN(service_date)) AS start_date
    FROM thalassa.row_counts_daily
),

daily_scope AS (
    SELECT
        d.service_date,
        d.rows_count,
        d.total_passengers,
        d.total_vehicles
    FROM thalassa.row_counts_daily d
    CROSS JOIN scope s
    WHERE d.service_date BETWEEN s.start_date AND s.end_date
),

totals AS (
    SELECT
        s.start_date,
        s.end_date,
        COALESCE(SUM(d.rows_count), 0) AS rows_total,
        COALESCE(SUM(d.total_passengers), 0) AS passengers_total,
        COALESCE(SUM(d.total_vehicles), 0) AS vehicles_total
    FROM scope s
    LEFT JOIN daily_scope d ON TRUE
    GROUP BY s.start_date, s.end_date
),

trend_enriched AS (
    SELECT
        COALESCE(AVG(IF(rn <= 7, total_passengers, NULL)), 0) AS recent_avg_passengers,
        COALESCE(AVG(IF(rn BETWEEN 8 AND 14, total_passengers, NULL)), 0) AS prior_avg_passengers
    FROM (
        SELECT
            service_date,
            total_passengers,
            ROW_NUMBER() OVER (ORDER BY service_date DESC) AS rn
        FROM daily_scope
    ) ranked
),

top_corridor AS (
    SELECT
        CONCAT(COALESCE(departure_port, 'Unknown'), ' -> ', COALESCE(arrival_port, 'Unknown')) AS pair_label,
        SUM(total_passengers) AS passengers,
        SUM(traffic_record_count) AS sailings
    FROM thalassa.fct_route_traffic_daily r
    CROSS JOIN scope s
    WHERE r.service_date BETWEEN s.start_date AND s.end_date
    GROUP BY 1
),

top_corridor_best AS (
    SELECT
        pair_label,
        passengers,
        sailings
    FROM top_corridor
    ORDER BY passengers DESC, sailings DESC
    LIMIT 1
),

top_route_digest AS (
    SELECT
        IFNULL(
            SUBSTR(
                LOWER(
                    TO_HEX(
                        SHA256(
                            STRING_AGG(
                                CONCAT(pair_label, ':', CAST(CAST(passengers AS INT64) AS STRING)),
                                '|' ORDER BY passengers DESC, pair_label ASC LIMIT 5
                            )
                        )
                    )
                ),
                1,
                16
            ),
            'none'
        ) AS top_routes_hash
    FROM top_corridor
),

top_port AS (
    SELECT
        port_name,
        SUM(total_departing_passengers + total_arriving_passengers) AS passengers
    FROM thalassa.fct_port_activity_daily p
    CROSS JOIN scope s
    WHERE p.service_date BETWEEN s.start_date AND s.end_date
    GROUP BY 1
),

top_port_best AS (
    SELECT
        port_name,
        passengers
    FROM top_port
    ORDER BY passengers DESC
    LIMIT 1
),

base AS (
    SELECT
        CURRENT_TIMESTAMP() AS snapshot_ts,
        t.start_date,
        t.end_date,
        t.rows_total,
        t.passengers_total,
        t.vehicles_total,
        te.recent_avg_passengers,
        te.prior_avg_passengers,
        COALESCE(tc.pair_label, 'No data') AS top_corridor_label,
        COALESCE(tc.passengers, 0) AS top_corridor_passengers,
        COALESCE(tc.sailings, 0) AS top_corridor_sailings,
        COALESCE(tp.port_name, 'No data') AS top_port_name,
        COALESCE(tp.passengers, 0) AS top_port_passengers,
        CONCAT(
            CAST(t.end_date AS STRING),
            '|rows:', CAST(t.rows_total AS STRING),
            '|pax:', CAST(t.passengers_total AS STRING),
            '|top:', COALESCE(tr.top_routes_hash, 'none')
        ) AS data_version
    FROM totals t
    CROSS JOIN trend_enriched te
    CROSS JOIN top_route_digest tr
    LEFT JOIN top_corridor_best tc ON TRUE
    LEFT JOIN top_port_best tp ON TRUE
)

-- The main agent_panel snapshot is written by auto_panel_snapshot_writer.py.
-- This SQL asset only seeds deepen-chart snapshot rows.
SELECT
    snapshot_ts,
    'traffic_pulse' AS chart_id,
    'deepen_chart' AS insight_type,
    'weekly' AS grain,
    CAST(NULL AS STRING) AS filter_hash,
    data_version,
    'Traffic Pulse Insight' AS title,
    FORMAT(
        'Recent 7-day passenger average is %s versus prior %s, indicating %s momentum.',
        CAST(CAST(recent_avg_passengers AS INT64) AS STRING),
        CAST(CAST(prior_avg_passengers AS INT64) AS STRING),
        CASE WHEN recent_avg_passengers >= prior_avg_passengers THEN 'upward' ELSE 'downward' END
    ) AS summary,
    ARRAY<STRING>[
        FORMAT('Recent 7-day average: %s passengers/day.', CAST(CAST(recent_avg_passengers AS INT64) AS STRING)),
        FORMAT('Prior 7-day average: %s passengers/day.', CAST(CAST(prior_avg_passengers AS INT64) AS STRING))
    ] AS facts,
    ARRAY<STRING>[
        IF(prior_avg_passengers > 0, FORMAT('Delta vs prior 7-day window: %+.1f%%.', ((recent_avg_passengers - prior_avg_passengers) / prior_avg_passengers) * 100), 'Insufficient prior window for delta.')
    ] AS anomalies,
    ARRAY<STRING>[
        FORMAT('Window total passengers: %s.', CAST(CAST(passengers_total AS INT64) AS STRING)),
        FORMAT('Window total vehicles: %s.', CAST(CAST(vehicles_total AS INT64) AS STRING))
    ] AS comparisons,
    ARRAY<STRING>[
        'Investigate major day-over-day swings against route concentration changes.'
    ] AS recommendations,
    FORMAT('Trend check: recent 7-day average %s vs prior %s.', CAST(CAST(recent_avg_passengers AS INT64) AS STRING), CAST(CAST(prior_avg_passengers AS INT64) AS STRING)) AS report_text,
    CAST(NULL AS STRING) AS insight_text,
    "deterministic_sql" AS generation_mode,
    CAST(NULL AS STRING) AS generation_error,
    CAST(NULL AS STRING) AS model_name,
    CAST(NULL AS STRING) AS source_snapshot_json,
    CAST(NULL AS STRING) AS overlay_json
FROM base

UNION ALL

SELECT
    snapshot_ts,
    'top_corridors' AS chart_id,
    'deepen_chart' AS insight_type,
    'weekly' AS grain,
    CAST(NULL AS STRING) AS filter_hash,
    data_version,
    'Top Corridors Insight' AS title,
    FORMAT('Leading corridor is %s with %s passengers and %s sailings in scope.', top_corridor_label, CAST(CAST(top_corridor_passengers AS INT64) AS STRING), CAST(CAST(top_corridor_sailings AS INT64) AS STRING)) AS summary,
    ARRAY<STRING>[
        FORMAT('Top corridor: %s.', top_corridor_label),
        FORMAT('Top corridor passengers: %s.', CAST(CAST(top_corridor_passengers AS INT64) AS STRING)),
        FORMAT('Top corridor sailings: %s.', CAST(CAST(top_corridor_sailings AS INT64) AS STRING))
    ] AS facts,
    ARRAY<STRING>[
        FORMAT('Top corridor share: %.1f%% of total passengers.', IF(passengers_total > 0, (top_corridor_passengers / passengers_total) * 100, 0))
    ] AS anomalies,
    ARRAY<STRING>[
        FORMAT('Total window passengers: %s.', CAST(CAST(passengers_total AS INT64) AS STRING))
    ] AS comparisons,
    ARRAY<STRING>[
        'Watch for concentration spikes that increase network dependency on one corridor.'
    ] AS recommendations,
    FORMAT('Corridor concentration summary: %s leads with %s passengers.', top_corridor_label, CAST(CAST(top_corridor_passengers AS INT64) AS STRING)) AS report_text,
    CAST(NULL AS STRING) AS insight_text,
    "deterministic_sql" AS generation_mode,
    CAST(NULL AS STRING) AS generation_error,
    CAST(NULL AS STRING) AS model_name,
    CAST(NULL AS STRING) AS source_snapshot_json,
    CAST(NULL AS STRING) AS overlay_json
FROM base

UNION ALL

SELECT
    snapshot_ts,
    'port_balance' AS chart_id,
    'deepen_chart' AS insight_type,
    'weekly' AS grain,
    CAST(NULL AS STRING) AS filter_hash,
    data_version,
    'Port Balance Insight' AS title,
    FORMAT('Highest passenger flow port is %s with %s passengers in the current window.', top_port_name, CAST(CAST(top_port_passengers AS INT64) AS STRING)) AS summary,
    ARRAY<STRING>[
        FORMAT('Top port: %s.', top_port_name),
        FORMAT('Top port passengers: %s.', CAST(CAST(top_port_passengers AS INT64) AS STRING))
    ] AS facts,
    ARRAY<STRING>[
        FORMAT('Top port share of total passengers: %.1f%%.', IF(passengers_total > 0, (top_port_passengers / passengers_total) * 100, 0))
    ] AS anomalies,
    ARRAY<STRING>[
        FORMAT('Total portscope passengers in window: %s.', CAST(CAST(passengers_total AS INT64) AS STRING))
    ] AS comparisons,
    ARRAY<STRING>[
        'Review weekday imbalance at the leading port and monitor abrupt inflow/outflow changes.'
    ] AS recommendations,
    FORMAT('Port focus summary: %s leads at %s passengers.', top_port_name, CAST(CAST(top_port_passengers AS INT64) AS STRING)) AS report_text,
    CAST(NULL AS STRING) AS insight_text,
    "deterministic_sql" AS generation_mode,
    CAST(NULL AS STRING) AS generation_error,
    CAST(NULL AS STRING) AS model_name,
    CAST(NULL AS STRING) AS source_snapshot_json,
    CAST(NULL AS STRING) AS overlay_json
FROM base;



