/* @bruin
name: thalassa.dim_port
type: bq.sql

depends:
  - thalassa.int_port_day

materialization:
  type: table

columns:
  - name: port_name
    type: string
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    port_name,
    COUNT(*) AS active_days,
    SUM(departure_record_count) AS total_departure_records,
    SUM(arrival_record_count) AS total_arrival_records,
    SUM(total_departing_passengers) AS total_departing_passengers,
    SUM(total_arriving_passengers) AS total_arriving_passengers,
    SUM(total_departing_vehicles) AS total_departing_vehicles,
    SUM(total_arriving_vehicles) AS total_arriving_vehicles
FROM thalassa.int_port_day
WHERE port_name IS NOT NULL
GROUP BY port_name;
