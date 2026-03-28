/* @bruin
name: thalassa.int_port_movement
type: bq.sql

depends:
  - thalassa.int_traffic_record

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - movement_type
    - port_name

columns:
  - name: event_id
    type: string
    checks:
      - name: not_null
  - name: service_date
    type: date
    checks:
      - name: not_null
  - name: movement_type
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - departure
          - arrival
  - name: port_name
    type: string
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    checks:
      - name: non_negative
  - name: vehicle_count
    type: integer
    checks:
      - name: non_negative
@bruin */

SELECT
    event_id,
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    'departure' AS movement_type,
    departure_port AS port_name,
    passenger_count,
    vehicle_count
FROM thalassa.int_traffic_record
WHERE departure_port IS NOT NULL

UNION ALL

SELECT
    event_id,
    service_date,
    service_week_start,
    service_month_start,
    service_year,
    'arrival' AS movement_type,
    arrival_port AS port_name,
    passenger_count,
    vehicle_count
FROM thalassa.int_traffic_record
WHERE arrival_port IS NOT NULL;
