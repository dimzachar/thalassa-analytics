/* @bruin
name: thalassa.stg_sailing_traffic
type: bq.sql

depends:
  - thalassa.raw_sailing_traffic

materialization:
  type: table
  partition_by: service_date
  cluster_by:
    - route_code
    - departure_port
    - arrival_port

columns:
  - name: passenger_count
    type: integer
    checks:
      - name: non_negative
  - name: vehicle_count
    type: integer
    checks:
      - name: non_negative

custom_checks:
  - name: passenger_count_safe_cast_did_not_drop_values
    description: Fail when a staged row has a non-empty raw passenger value that cannot be converted to INTEGER.
    query: |
      SELECT COUNT(*)
      FROM thalassa.raw_sailing_traffic
      WHERE SAFE_CAST(JSON_VALUE(raw_payload, '$.date') AS DATE) IS NOT NULL
        AND NULLIF(TRIM(JSON_VALUE(raw_payload, '$.passengercount')), '') IS NOT NULL
        AND SAFE_CAST(JSON_VALUE(raw_payload, '$.passengercount') AS INTEGER) IS NULL
    value: 0
  - name: vehicle_count_safe_cast_did_not_drop_values
    description: Fail when a staged row has a non-empty raw vehicle value that cannot be converted to INTEGER.
    query: |
      SELECT COUNT(*)
      FROM thalassa.raw_sailing_traffic
      WHERE SAFE_CAST(JSON_VALUE(raw_payload, '$.date') AS DATE) IS NOT NULL
        AND NULLIF(TRIM(JSON_VALUE(raw_payload, '$.vehiclecount')), '') IS NOT NULL
        AND SAFE_CAST(JSON_VALUE(raw_payload, '$.vehiclecount') AS INTEGER) IS NULL
    value: 0
@bruin */

SELECT
    record_hash,
    record_hash AS source_record_id,
    SAFE_CAST(service_date AS DATE) AS service_date,
    NULLIF(REGEXP_REPLACE(TRIM(route_code), r'\s+', ' '), '') AS route_code,
    NULLIF(
        REGEXP_REPLACE(
            TRIM(
                COALESCE(
                    route_name,
                    JSON_VALUE(raw_payload, '$.routecodenames'),
                    JSON_VALUE(raw_payload, '$.route_name'),
                    JSON_VALUE(raw_payload, '$.route')
                )
            ),
            r'\s+',
            ' '
        ),
        ''
    ) AS route_name,
    NULLIF(
        TRANSLATE(
            UPPER(REGEXP_REPLACE(TRIM(departure_port), r'\s+', ' ')),
            'ΆΈΉΊΌΎΏΪΫ',
            'ΑΕΗΙΟΥΩΙΥ'
        ),
        ''
    ) AS departure_port,
    NULLIF(
        TRANSLATE(
            UPPER(REGEXP_REPLACE(TRIM(arrival_port), r'\s+', ' ')),
            'ΆΈΉΊΌΎΏΪΫ',
            'ΑΕΗΙΟΥΩΙΥ'
        ),
        ''
    ) AS arrival_port,
    SAFE_CAST(passenger_count AS INTEGER) AS passenger_count,
    SAFE_CAST(vehicle_count AS INTEGER) AS vehicle_count,
    SAFE_CAST(ingested_at AS TIMESTAMP) AS ingested_at,
    raw_payload
FROM thalassa.raw_sailing_traffic
WHERE SAFE_CAST(service_date AS DATE) IS NOT NULL;
