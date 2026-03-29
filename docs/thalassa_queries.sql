-- Bruin query reference for the Thalassa pipeline.
-- Default connection is gcp-default (BigQuery); swap to --connection duckdb-default and replace
-- table references with schema-prefixed names (e.g. ingestion.raw_sailing_traffic) to run locally against DuckDB.

-- Count rows in raw_sailing_traffic
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.raw_sailing_traffic" --description "count rows in raw sailing traffic ingestion table"

-- Count rows in stg_sailing_traffic
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.stg_sailing_traffic" --description "count rows in staging sailing traffic table"

-- Count rows in row_counts_daily
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_daily" --description "count rows in daily row count report"

-- Count rows in row_counts_weekly
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_weekly" --description "count rows in weekly row count report"

-- Count rows in fct_route_traffic_daily
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_daily" --description "count rows in daily route fact table"

-- Count rows in fct_port_activity_daily
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT COUNT(*) FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_port_activity_daily" --description "count rows in daily port fact table"

-- Sample raw ingestion rows
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT * FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.raw_sailing_traffic LIMIT 10" --description "inspect raw ingestion rows"

-- Sample staging rows
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT * FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.stg_sailing_traffic LIMIT 10" --description "inspect staging rows"

-- Sample route daily mart rows
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT * FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_daily LIMIT 10" --description "inspect daily route mart rows"

-- Sample port daily mart rows
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT * FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_port_activity_daily LIMIT 10" --description "inspect daily port mart rows"

-- Check date range in staging
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT MIN(service_date) AS min_date, MAX(service_date) AS max_date FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.stg_sailing_traffic" --description "check date coverage in staging"

-- Daily traffic totals
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT service_date, rows_count, total_passengers, total_vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_daily ORDER BY service_date" --description "inspect daily totals"

-- Weekly traffic totals
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT week_start, rows_count, total_passengers, total_vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_weekly ORDER BY week_start" --description "inspect weekly totals"

-- Monthly traffic totals
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT year_month, rows_count, total_passengers, total_vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_monthly ORDER BY year_month" --description "inspect monthly totals"

-- Top 10 routes by passengers
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT route_name, SUM(total_passengers) AS passengers, SUM(total_vehicles) AS vehicles, SUM(traffic_record_count) AS sailings FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_daily GROUP BY route_name ORDER BY passengers DESC LIMIT 10" --description "find top routes by passengers"

-- Top 10 routes by vehicles
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT route_name, SUM(total_vehicles) AS vehicles, SUM(total_passengers) AS passengers, SUM(traffic_record_count) AS sailings FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_daily GROUP BY route_name ORDER BY vehicles DESC LIMIT 10" --description "find top routes by vehicles"

-- Top 10 ports by departures
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT port_name, SUM(departure_record_count) AS departures, SUM(arrival_record_count) AS arrivals FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_port_activity_daily GROUP BY port_name ORDER BY departures DESC LIMIT 10" --description "find top ports by departures"

-- Top 10 ports by arrivals
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT port_name, SUM(arrival_record_count) AS arrivals, SUM(departure_record_count) AS departures FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_port_activity_daily GROUP BY port_name ORDER BY arrivals DESC LIMIT 10" --description "find top ports by arrivals"

-- Top departure-arrival pairs by passengers
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT departure_port, arrival_port, COUNT(*) AS route_events, SUM(passenger_count) AS passengers, SUM(vehicle_count) AS vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.stg_sailing_traffic GROUP BY departure_port, arrival_port ORDER BY passengers DESC LIMIT 10" --description "find top departure-arrival pairs by passengers"

-- Average passengers and vehicles per route
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT route_name, AVG(avg_passengers_per_record) AS avg_passengers_per_sailing, AVG(avg_vehicles_per_record) AS avg_vehicles_per_sailing, SUM(traffic_record_count) AS sailings FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_daily GROUP BY route_name ORDER BY avg_passengers_per_sailing DESC LIMIT 10" --description "find routes with highest average load"

-- Weekly totals matching official portal grain
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT week_start, total_passengers, total_vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_weekly ORDER BY week_start DESC LIMIT 20" --description "compare weekly totals with official portal"

-- Check the official weekly point for 2026-03-16
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT week_start, total_passengers, total_vehicles FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.row_counts_weekly WHERE week_start = DATE '2026-03-16'" --description "check official weekly totals for 2026-03-16"

-- Top route-week combinations
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT week_start, route_name, total_passengers, total_vehicles, traffic_record_count FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_route_traffic_weekly ORDER BY total_passengers DESC LIMIT 20" --description "find top route-week combinations"

-- Top port-week combinations
bruin query --connection gcp-default --config-file .\.bruin.yml --query "SELECT week_start, port_name, departure_record_count, arrival_record_count, total_departing_passengers, total_arriving_passengers FROM YOUR_GCP_PROJECT.YOUR_THALASSA_BQ_DATASET.fct_port_activity_weekly ORDER BY departure_record_count DESC LIMIT 20" --description "find top port-week combinations"
