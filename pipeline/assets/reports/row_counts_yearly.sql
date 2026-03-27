/* @bruin
name: thalassa.row_counts_yearly
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table

columns:
  - name: year
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: rows_count
    type: integer
    checks:
      - name: positive
  - name: distinct_itineraries
    type: integer
    checks:
      - name: positive
  - name: distinct_route_pairs
    type: integer
    checks:
      - name: positive
  - name: total_passengers
    type: integer
    checks:
      - name: non_negative
  - name: total_vehicles
    type: integer
    checks:
      - name: non_negative
@bruin */

SELECT
    service_year AS year,
    COUNT(*) AS rows_count,
    COUNT(DISTINCT route_key) AS distinct_itineraries,
    COUNT(DISTINCT route_pair_key) AS distinct_route_pairs,
    COUNT(DISTINCT departure_port) AS distinct_departure_ports,
    COUNT(DISTINCT arrival_port) AS distinct_arrival_ports,
    COALESCE(SUM(passenger_count), 0) AS total_passengers,
    COALESCE(SUM(vehicle_count), 0) AS total_vehicles
FROM thalassa.int_traffic_record
GROUP BY service_year;
