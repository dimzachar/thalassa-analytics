/* @bruin
name: thalassa.row_counts_weekly
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table
  partition_by: week_start
@bruin */

SELECT
    service_week_start AS week_start,
    COUNT(*) AS rows_count,
    COUNT(DISTINCT route_key) AS distinct_itineraries,
    COUNT(DISTINCT route_pair_key) AS distinct_route_pairs,
    COUNT(DISTINCT departure_port) AS distinct_departure_ports,
    COUNT(DISTINCT arrival_port) AS distinct_arrival_ports,
    COALESCE(SUM(passenger_count), 0) AS total_passengers,
    COALESCE(SUM(vehicle_count), 0) AS total_vehicles
FROM thalassa.int_traffic_record
GROUP BY service_week_start;
