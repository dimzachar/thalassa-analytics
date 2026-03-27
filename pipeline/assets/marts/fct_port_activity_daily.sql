/* @bruin
name: thalassa.fct_port_activity_daily
type: bq.sql

depends:
  - thalassa.int_port_day

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - port_name

columns:
  - name: service_date
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
    service_date,
    port_name,
    departure_record_count,
    arrival_record_count,
    total_departing_passengers,
    total_arriving_passengers,
    total_departing_vehicles,
    total_arriving_vehicles
FROM thalassa.int_port_day;
