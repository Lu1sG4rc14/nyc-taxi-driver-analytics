-- Description: Creates BigQuery tables for the NYC taxi analytics pipeline.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)

-- Operational manifest used for idempotency, auditability, and delta decisions.
CREATE TABLE IF NOT EXISTS `{project_id}.{ops_dataset}.ingestion_manifest` (
  run_id STRING NOT NULL,
  source_name STRING NOT NULL,
  source_month DATE NOT NULL,
  source_url STRING NOT NULL,
  source_etag STRING,
  source_last_modified STRING,
  source_content_length INT64,
  source_row_count INT64,
  previous_row_count INT64,
  delta_row_count INT64,
  snapshot_hash STRING,
  previous_snapshot_hash STRING,
  raw_gcs_uri STRING,
  delta_gcs_uri STRING,
  staging_table STRING,
  load_mode STRING NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  error_message STRING
)
PARTITION BY DATE(started_at)
CLUSTER BY source_name, source_month, status;

-- Small public reference table reloaded from the TLC zone lookup CSV.
CREATE TABLE IF NOT EXISTS `{project_id}.{bronze_dataset}.taxi_zone_lookup` (
  location_id INT64 NOT NULL,
  borough STRING,
  zone STRING,
  service_zone STRING,
  meta_loaded_at TIMESTAMP
);

-- Bronze preserves canonicalized source fields first, then generated metadata.
CREATE TABLE IF NOT EXISTS `{project_id}.{bronze_dataset}.yellow_trips_raw` (
  vendor_id INT64,
  pickup_datetime DATETIME,
  dropoff_datetime DATETIME,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  ratecode_id INT64,
  store_and_fwd_flag STRING,
  pickup_location_id INT64,
  dropoff_location_id INT64,
  payment_type INT64,
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  improvement_surcharge NUMERIC,
  total_amount NUMERIC,
  congestion_surcharge NUMERIC,
  airport_fee NUMERIC,
  -- calc_* fields are deterministic helpers derived from pickup/dropoff timestamps.
  calc_pickup_date DATE,
  calc_pickup_hour INT64,
  calc_dropoff_date DATE,
  -- meta_* fields capture lineage needed for replay, auditing, and deduplication.
  meta_trip_key STRING NOT NULL,
  meta_source_row_number INT64 NOT NULL,
  meta_source_month DATE NOT NULL,
  meta_source_file_url STRING,
  meta_source_file_etag STRING,
  meta_ingestion_batch_id STRING,
  meta_ingested_at TIMESTAMP
)
PARTITION BY meta_source_month
CLUSTER BY pickup_location_id, dropoff_location_id, calc_pickup_hour, payment_type;

-- Silver applies typed naming, zone enrichment, business metrics, and quality flags.
CREATE TABLE IF NOT EXISTS `{project_id}.{silver_dataset}.fact_trips` (
  vendor_id INT64,
  pickup_dt DATETIME,
  dropoff_dt DATETIME,
  passenger_count_vl FLOAT64,
  trip_distance_miles_vl FLOAT64,
  ratecode_id INT64,
  store_and_fwd_flag_cd STRING,
  pickup_location_id INT64,
  dropoff_location_id INT64,
  payment_type_id INT64,
  fare_amount_vl NUMERIC,
  extra_amount_vl NUMERIC,
  mta_tax_amount_vl NUMERIC,
  tip_amount_vl NUMERIC,
  tolls_amount_vl NUMERIC,
  improvement_surcharge_amount_vl NUMERIC,
  total_amount_vl NUMERIC,
  congestion_surcharge_amount_vl NUMERIC,
  airport_fee_amount_vl NUMERIC,
  -- meta_* columns preserve source lineage after cleaning and enrichment.
  meta_trip_id STRING NOT NULL,
  meta_source_month_dt DATE NOT NULL,
  meta_source_row_number_vl INT64 NOT NULL,
  meta_ingested_at_dt TIMESTAMP,
  calc_pickup_date_dt DATE,
  calc_pickup_hour_vl INT64,
  calc_dropoff_date_dt DATE,
  calc_pickup_day_of_week_vl INT64,
  calc_pickup_day_name_ds STRING,
  calc_duration_minutes_vl FLOAT64,
  -- geo_* columns are added from taxi_zone_lookup.
  geo_pickup_borough_ds STRING,
  geo_pickup_zone_ds STRING,
  geo_pickup_service_zone_ds STRING,
  geo_dropoff_borough_ds STRING,
  geo_dropoff_zone_ds STRING,
  geo_dropoff_service_zone_ds STRING,
  calc_payment_type_name_ds STRING,
  calc_tip_pct_vl FLOAT64,
  calc_earnings_per_hour_vl FLOAT64,
  calc_earnings_per_mile_vl FLOAT64,
  -- flag_* columns encode analytical categories used by gold marts.
  flag_airport_pickup_lg BOOL,
  flag_airport_dropoff_lg BOOL,
  -- dq_* columns make quality filtering explicit instead of deleting raw records.
  dq_analysis_eligible_lg BOOL,
  dq_issue_count_vl INT64,
  dq_flags_ds STRING
)
PARTITION BY meta_source_month_dt
CLUSTER BY pickup_location_id, dropoff_location_id, calc_pickup_hour_vl, payment_type_id;

-- Gold mart: zone and hour profitability patterns.
CREATE TABLE IF NOT EXISTS `{project_id}.{gold_dataset}.zone_hour_earnings` (
  source_month_dt DATE NOT NULL,
  pickup_borough_ds STRING,
  pickup_zone_ds STRING,
  pickup_hour_vl INT64,
  trips_vl INT64,
  avg_total_amount_vl FLOAT64,
  avg_tip_amount_vl FLOAT64,
  avg_tip_pct_vl FLOAT64,
  avg_trip_distance_vl FLOAT64,
  avg_duration_minutes_vl FLOAT64,
  avg_earnings_per_hour_vl FLOAT64,
  median_earnings_per_hour_vl FLOAT64,
  p75_earnings_per_hour_vl FLOAT64,
  opportunity_score_vl FLOAT64
)
PARTITION BY source_month_dt
CLUSTER BY pickup_borough_ds, pickup_zone_ds, pickup_hour_vl;

-- Gold mart: airport-related pickup/dropoff strategy.
CREATE TABLE IF NOT EXISTS `{project_id}.{gold_dataset}.airport_strategy` (
  source_month_dt DATE NOT NULL,
  pickup_hour_vl INT64,
  pickup_zone_ds STRING,
  flag_airport_pickup_lg BOOL,
  flag_airport_dropoff_lg BOOL,
  trips_vl INT64,
  avg_total_amount_vl FLOAT64,
  avg_duration_minutes_vl FLOAT64,
  avg_earnings_per_hour_vl FLOAT64,
  avg_tip_pct_vl FLOAT64
)
PARTITION BY source_month_dt
CLUSTER BY pickup_hour_vl, pickup_zone_ds;

-- Gold mart: tipping behavior by payment method and pickup context.
CREATE TABLE IF NOT EXISTS `{project_id}.{gold_dataset}.payment_tip_patterns` (
  source_month_dt DATE NOT NULL,
  pickup_borough_ds STRING,
  pickup_hour_vl INT64,
  payment_type_name_ds STRING,
  trips_vl INT64,
  avg_total_amount_vl FLOAT64,
  avg_tip_amount_vl FLOAT64,
  avg_tip_pct_vl FLOAT64,
  credit_card_share_vl FLOAT64
)
PARTITION BY source_month_dt
CLUSTER BY pickup_borough_ds, pickup_hour_vl, payment_type_name_ds;

-- Gold mart: final ranked advice rows built from analytical marts.
CREATE TABLE IF NOT EXISTS `{project_id}.{gold_dataset}.driver_recommendations` (
  source_month_dt DATE NOT NULL,
  recommendation_rank_vl INT64,
  recommendation_type_ds STRING,
  advice_ds STRING,
  evidence_ds STRING,
  opportunity_score_vl FLOAT64
)
PARTITION BY source_month_dt
CLUSTER BY recommendation_type_ds, recommendation_rank_vl;
