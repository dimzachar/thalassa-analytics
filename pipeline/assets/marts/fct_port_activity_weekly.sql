/* @bruin
name: thalassa.fct_port_activity_weekly
type: bq.sql

depends:
  - thalassa.fct_port_activity_daily

materialization:
  type: table
  partition_by: week_start
  cluster_by:
    - port_name

columns:
  - name: week_start
    type: date
    checks:
      - name: not_null
  - name: port_name
    type: string
    checks:
      - name: not_null
  - name: departure_record_count
    type: integer
    checks:
      - name: non_negative
  - name: arrival_record_count
    type: integer
    checks:
      - name: non_negative
  - name: total_departing_passengers
    type: integer
    checks:
      - name: non_negative
  - name: total_arriving_passengers
    type: integer
    checks:
      - name: non_negative
  - name: total_departing_vehicles
    type: integer
    checks:
      - name: non_negative
  - name: total_arriving_vehicles
    type: integer
    checks:
      - name: non_negative
@bruin */

SELECT
    DATE_TRUNC(service_date, WEEK(MONDAY)) AS week_start,
    port_name,
    SUM(departure_record_count) AS departure_record_count,
    SUM(arrival_record_count) AS arrival_record_count,
    SUM(total_departing_passengers) AS total_departing_passengers,
    SUM(total_arriving_passengers) AS total_arriving_passengers,
    SUM(total_departing_vehicles) AS total_departing_vehicles,
    SUM(total_arriving_vehicles) AS total_arriving_vehicles
FROM thalassa.fct_port_activity_daily
GROUP BY
    DATE_TRUNC(service_date, WEEK(MONDAY)),
    port_name;
