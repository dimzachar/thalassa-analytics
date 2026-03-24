/* @bruin
name: thalassa.row_counts_daily
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table
  partition_by: service_date
@bruin */

SELECT
    service_date,
    COUNT(*) AS rows_count,
    COUNT(DISTINCT route_key) AS distinct_itineraries,
    COUNT(DISTINCT route_pair_key) AS distinct_route_pairs,
    COUNT(DISTINCT departure_port) AS distinct_departure_ports,
    COUNT(DISTINCT arrival_port) AS distinct_arrival_ports,
    COALESCE(SUM(passenger_count), 0) AS total_passengers,
    COALESCE(SUM(vehicle_count), 0) AS total_vehicles
FROM thalassa.int_traffic_record
GROUP BY service_date;
