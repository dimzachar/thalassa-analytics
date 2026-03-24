/* @bruin
name: thalassa.fct_port_activity_monthly
type: bq.sql

depends:
  - thalassa.fct_port_activity_daily

materialization:
  type: table
  partition_by: year_month
  cluster_by:
    - port_name
@bruin */

SELECT
    DATE_TRUNC(service_date, MONTH) AS year_month,
    port_name,
    SUM(departure_record_count) AS departure_record_count,
    SUM(arrival_record_count) AS arrival_record_count,
    SUM(total_departing_passengers) AS total_departing_passengers,
    SUM(total_arriving_passengers) AS total_arriving_passengers,
    SUM(total_departing_vehicles) AS total_departing_vehicles,
    SUM(total_arriving_vehicles) AS total_arriving_vehicles
FROM thalassa.fct_port_activity_daily
GROUP BY
    DATE_TRUNC(service_date, MONTH),
    port_name;
