/* @bruin
name: thalassa.int_port_day
type: bq.sql

depends:
  - thalassa.int_port_movement

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - port_name
@bruin */

SELECT
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    port_name,
    SUM(CASE WHEN movement_type = 'departure' THEN 1 ELSE 0 END) AS departure_record_count,
    SUM(CASE WHEN movement_type = 'arrival' THEN 1 ELSE 0 END) AS arrival_record_count,
    COALESCE(SUM(CASE WHEN movement_type = 'departure' THEN passenger_count ELSE 0 END), 0) AS total_departing_passengers,
    COALESCE(SUM(CASE WHEN movement_type = 'arrival' THEN passenger_count ELSE 0 END), 0) AS total_arriving_passengers,
    COALESCE(SUM(CASE WHEN movement_type = 'departure' THEN vehicle_count ELSE 0 END), 0) AS total_departing_vehicles,
    COALESCE(SUM(CASE WHEN movement_type = 'arrival' THEN vehicle_count ELSE 0 END), 0) AS total_arriving_vehicles
FROM thalassa.int_port_movement
GROUP BY
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    port_name;
