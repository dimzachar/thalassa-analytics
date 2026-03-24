/* @bruin
name: thalassa.int_traffic_record
type: bq.sql

depends:
  - thalassa.stg_sailing_traffic

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - route_pair_key
    - departure_port
    - arrival_port

columns:
  - name: event_id
    type: string
    checks:
      - name: not_null
      - name: unique
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

custom_checks:
  - name: route_key_has_no_hash_collisions
    description: Fail when one hashed route_key maps to multiple natural itinerary keys.
    query: |
      SELECT COUNT(*)
      FROM (
          SELECT route_key
          FROM thalassa.int_traffic_record
          GROUP BY route_key
          HAVING COUNT(
              DISTINCT COALESCE(
                  route_code,
                  route_name,
                  CONCAT(
                      'unknown_itinerary|',
                      COALESCE(departure_port, 'unknown_departure'),
                      '|',
                      COALESCE(arrival_port, 'unknown_arrival')
                  )
              )
          ) > 1
      )
    value: 0
  - name: route_pair_key_has_no_hash_collisions
    description: Fail when one hashed route_pair_key maps to multiple natural route+port-pair keys.
    query: |
      SELECT COUNT(*)
      FROM (
          SELECT route_pair_key
          FROM thalassa.int_traffic_record
          GROUP BY route_pair_key
          HAVING COUNT(
              DISTINCT CONCAT(
                  COALESCE(route_code, route_name, ''),
                  '|',
                  COALESCE(departure_port, ''),
                  '|',
                  COALESCE(arrival_port, '')
              )
          ) > 1
      )
    value: 0
  - name: route_pair_key_maps_to_single_route_key
    description: Fail when one route_pair_key maps to multiple route_key values.
    query: |
      SELECT COUNT(*)
      FROM (
          SELECT route_pair_key
          FROM thalassa.int_traffic_record
          GROUP BY route_pair_key
          HAVING COUNT(DISTINCT route_key) > 1
      )
    value: 0
@bruin */

WITH ranked AS (
    SELECT
        record_hash,
        source_record_id,
        service_date,
        route_code,
        route_name,
        departure_port,
        arrival_port,
        passenger_count,
        vehicle_count,
        ingested_at,
        raw_payload,
        ROW_NUMBER() OVER (
            PARTITION BY record_hash
            ORDER BY ingested_at DESC, raw_payload DESC
        ) AS row_num
    FROM thalassa.stg_sailing_traffic
),
deduplicated AS (
    SELECT
        record_hash,
        source_record_id,
        service_date,
        route_code,
        route_name,
        departure_port,
        arrival_port,
        passenger_count,
        vehicle_count,
        ingested_at,
        raw_payload
    FROM ranked
    WHERE row_num = 1
),
identified AS (
    SELECT
        record_hash,
        source_record_id,
        service_date,
        route_code,
        route_name,
        departure_port,
        arrival_port,
        passenger_count,
        vehicle_count,
        ingested_at,
        raw_payload,
        LOWER(TO_HEX(
            SHA256(
                COALESCE(
                    route_code,
                    route_name,
                    CONCAT(
                        'unknown_itinerary|',
                        COALESCE(departure_port, 'unknown_departure'),
                        '|',
                        COALESCE(arrival_port, 'unknown_arrival')
                    )
                )
            )
        )) AS canonical_route_key,
        LOWER(TO_HEX(
            SHA256(
                CONCAT(
                    COALESCE(route_code, route_name, ''),
                    '|',
                    COALESCE(departure_port, ''),
                    '|',
                    COALESCE(arrival_port, '')
                )
            )
        )) AS canonical_route_pair_key
    FROM deduplicated
)
SELECT
    record_hash AS event_id,
    source_record_id,
    service_date,
    DATE_TRUNC(service_date, WEEK(MONDAY)) AS service_week_start,
    DATE_TRUNC(service_date, MONTH) AS service_month_start,
    EXTRACT(YEAR FROM service_date) AS service_year,
    EXTRACT(MONTH FROM service_date) AS service_month,
    CASE
        WHEN EXTRACT(MONTH FROM service_date) IN (12, 1, 2) THEN 'winter'
        WHEN EXTRACT(MONTH FROM service_date) IN (3, 4, 5) THEN 'spring'
        WHEN EXTRACT(MONTH FROM service_date) IN (6, 7, 8) THEN 'summer'
        ELSE 'autumn'
    END AS service_season,
    canonical_route_key AS route_key,
    canonical_route_pair_key AS route_pair_key,
    route_code,
    COALESCE(
        route_name,
        CONCAT(COALESCE(departure_port, 'Unknown departure'), ' -> ', COALESCE(arrival_port, 'Unknown arrival'))
    ) AS route_name,
    departure_port,
    arrival_port,
    passenger_count,
    vehicle_count,
    ingested_at,
    raw_payload
FROM identified;
