/* @bruin
name: thalassa.dim_route
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table

columns:
  - name: route_key
    type: string
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    route_key,
    MAX(route_code) AS route_code,
    MAX(route_name) AS route_name,
    COUNT(DISTINCT CONCAT(COALESCE(departure_port, ''), '|', COALESCE(arrival_port, ''))) AS distinct_route_pairs,
    CASE
        WHEN MAX(route_name) LIKE '%-%-%' THEN TRUE
        ELSE FALSE
    END AS is_multi_stop_route
FROM thalassa.int_traffic_record
GROUP BY route_key;
