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
