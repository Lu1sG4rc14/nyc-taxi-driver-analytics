-- Description: Replaces one source month in bronze from a full staged snapshot.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)

BEGIN TRANSACTION;

-- Rebuilds are scoped to one monthly partition to avoid rewriting history.
DELETE FROM `{project_id}.{bronze_dataset}.yellow_trips_raw`
WHERE meta_source_month = DATE('{source_month}-01');

INSERT INTO `{project_id}.{bronze_dataset}.yellow_trips_raw` (
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pickup_location_id,
  dropoff_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  calc_pickup_date,
  calc_pickup_hour,
  calc_dropoff_date,
  meta_trip_key,
  meta_source_row_number,
  meta_source_month,
  meta_source_file_url,
  meta_source_file_etag,
  meta_ingestion_batch_id,
  meta_ingested_at
)
WITH normalized AS (
  -- Staging is a run-scoped safety buffer. Bronze receives only rows that
  -- match the target source month and can be safely cast to the canonical schema.
  SELECT
    SAFE_CAST(vendor_id AS INT64) AS vendor_id,
    SAFE_CAST(pickup_datetime AS DATETIME) AS pickup_datetime,
    SAFE_CAST(dropoff_datetime AS DATETIME) AS dropoff_datetime,
    SAFE_CAST(passenger_count AS FLOAT64) AS passenger_count,
    SAFE_CAST(trip_distance AS FLOAT64) AS trip_distance,
    SAFE_CAST(ratecode_id AS INT64) AS ratecode_id,
    SAFE_CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    SAFE_CAST(pickup_location_id AS INT64) AS pickup_location_id,
    SAFE_CAST(dropoff_location_id AS INT64) AS dropoff_location_id,
    SAFE_CAST(payment_type AS INT64) AS payment_type,
    SAFE_CAST(fare_amount AS NUMERIC) AS fare_amount,
    SAFE_CAST(extra AS NUMERIC) AS extra,
    SAFE_CAST(mta_tax AS NUMERIC) AS mta_tax,
    SAFE_CAST(tip_amount AS NUMERIC) AS tip_amount,
    SAFE_CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    SAFE_CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    SAFE_CAST(total_amount AS NUMERIC) AS total_amount,
    SAFE_CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,
    SAFE_CAST(airport_fee AS NUMERIC) AS airport_fee,
    DATE(SAFE_CAST(pickup_datetime AS DATETIME)) AS calc_pickup_date,
    EXTRACT(HOUR FROM SAFE_CAST(pickup_datetime AS DATETIME)) AS calc_pickup_hour,
    DATE(SAFE_CAST(dropoff_datetime AS DATETIME)) AS calc_dropoff_date,
    -- The TLC source does not provide a stable trip ID, so the internal key is
    -- derived from source month and source row number after append/rebuild checks.
    TO_HEX(SHA256(CONCAT(CAST(meta_source_month AS STRING), '#', CAST(meta_source_row_number AS STRING)))) AS meta_trip_key,
    SAFE_CAST(meta_source_row_number AS INT64) AS meta_source_row_number,
    SAFE_CAST(meta_source_month AS DATE) AS meta_source_month,
    SAFE_CAST(meta_source_file_url AS STRING) AS meta_source_file_url,
    SAFE_CAST(meta_source_file_etag AS STRING) AS meta_source_file_etag,
    SAFE_CAST(meta_ingestion_batch_id AS STRING) AS meta_ingestion_batch_id,
    SAFE_CAST(meta_ingested_at AS TIMESTAMP) AS meta_ingested_at
  FROM `{project_id}.{staging_dataset}.{staging_table}`
  WHERE SAFE_CAST(meta_source_month AS DATE) = DATE('{source_month}-01')
)
SELECT *
FROM normalized;

COMMIT TRANSACTION;
