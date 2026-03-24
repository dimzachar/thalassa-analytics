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
