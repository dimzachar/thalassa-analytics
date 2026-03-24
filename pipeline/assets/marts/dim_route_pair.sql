/* @bruin
name: thalassa.dim_route_pair
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table
@bruin */

SELECT
    route_pair_key,
    route_key,
    MAX(route_code) AS route_code,
    MAX(route_name) AS route_name,
    MAX(departure_port) AS departure_port,
    MAX(arrival_port) AS arrival_port,
    COUNT(*) AS traffic_record_count
FROM thalassa.int_traffic_record
GROUP BY
    route_pair_key,
    route_key;
