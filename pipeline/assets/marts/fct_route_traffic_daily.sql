/* @bruin
name: thalassa.fct_route_traffic_daily
type: bq.sql

depends:
  - thalassa.int_route_day

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - departure_port
    - arrival_port
    - route_pair_key

columns:
  - name: service_date
    type: date
    checks:
      - name: not_null
  - name: route_key
    type: string
    checks:
      - name: not_null
  - name: route_pair_key
    type: string
    checks:
      - name: not_null
  - name: departure_port
    type: string
    checks:
      - name: not_null
  - name: arrival_port
    type: string
    checks:
      - name: not_null
  - name: traffic_record_count
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
    service_date,
    route_key,
    route_pair_key,
    route_code,
    route_name,
    departure_port,
    arrival_port,
    traffic_record_count,
    total_passengers,
    total_vehicles,
    avg_passengers_per_record,
    avg_vehicles_per_record
FROM thalassa.int_route_day;
