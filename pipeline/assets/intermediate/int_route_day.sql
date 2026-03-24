/* @bruin
name: thalassa.int_route_day
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - departure_port
    - arrival_port
    - route_pair_key
@bruin */

SELECT
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    route_key,
    route_pair_key,
    route_code,
    route_name,
    departure_port,
    arrival_port,
    COUNT(*) AS traffic_record_count,
    COALESCE(SUM(passenger_count), 0) AS total_passengers,
    COALESCE(SUM(vehicle_count), 0) AS total_vehicles,
    ROUND(COALESCE(AVG(passenger_count), 0), 2) AS avg_passengers_per_record,
    ROUND(COALESCE(AVG(vehicle_count), 0), 2) AS avg_vehicles_per_record
FROM thalassa.int_traffic_record
GROUP BY
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    route_key,
    route_pair_key,
    route_code,
    route_name,
    departure_port,
    arrival_port;
