/* @bruin
name: thalassa.fct_route_traffic_weekly
type: bq.sql

depends:
  - thalassa.fct_route_traffic_daily

materialization:
  type: table
  partition_by: week_start
  cluster_by:
    - departure_port
    - arrival_port
    - route_pair_key
@bruin */

SELECT
    DATE_TRUNC(service_date, WEEK(MONDAY)) AS week_start,
    route_key,
    route_pair_key,
    route_code,
    route_name,
    departure_port,
    arrival_port,
    SUM(traffic_record_count) AS traffic_record_count,
    COALESCE(SUM(total_passengers), 0) AS total_passengers,
    COALESCE(SUM(total_vehicles), 0) AS total_vehicles,
    ROUND(COALESCE(SUM(total_passengers) * 1.0 / NULLIF(SUM(traffic_record_count), 0), 0), 2) AS avg_passengers_per_record,
    ROUND(COALESCE(SUM(total_vehicles) * 1.0 / NULLIF(SUM(traffic_record_count), 0), 0), 2) AS avg_vehicles_per_record
FROM thalassa.fct_route_traffic_daily
GROUP BY
    DATE_TRUNC(service_date, WEEK(MONDAY)),
    route_key,
    route_pair_key,
    route_code,
    route_name,
    departure_port,
    arrival_port;
