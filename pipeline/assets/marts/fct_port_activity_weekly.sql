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
