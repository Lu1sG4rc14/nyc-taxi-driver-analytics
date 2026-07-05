-- Description: Rebuilds the cleaned and enriched trip fact table for one month.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)

BEGIN TRANSACTION;

-- Rebuilds are partition-scoped so corrections only affect the selected month.
DELETE FROM `{project_id}.{silver_dataset}.fact_trips`
WHERE meta_source_month_dt = DATE('{source_month}-01');

INSERT INTO `{project_id}.{silver_dataset}.fact_trips`
WITH enriched AS (
  SELECT
    r.vendor_id,
    r.pickup_datetime AS pickup_dt,
    r.dropoff_datetime AS dropoff_dt,
    r.passenger_count AS passenger_count_vl,
    r.trip_distance AS trip_distance_miles_vl,
    r.ratecode_id,
    r.store_and_fwd_flag AS store_and_fwd_flag_cd,
    r.pickup_location_id,
    r.dropoff_location_id,
    r.payment_type AS payment_type_id,
    r.fare_amount AS fare_amount_vl,
    r.extra AS extra_amount_vl,
    r.mta_tax AS mta_tax_amount_vl,
    r.tip_amount AS tip_amount_vl,
    r.tolls_amount AS tolls_amount_vl,
    r.improvement_surcharge AS improvement_surcharge_amount_vl,
    r.total_amount AS total_amount_vl,
    r.congestion_surcharge AS congestion_surcharge_amount_vl,
    r.airport_fee AS airport_fee_amount_vl,
    r.meta_trip_key AS meta_trip_id,
    r.meta_source_month AS meta_source_month_dt,
    r.meta_source_row_number AS meta_source_row_number_vl,
    r.meta_ingested_at AS meta_ingested_at_dt,
    -- calc_* fields derive reusable temporal features from source timestamps.
    r.calc_pickup_date AS calc_pickup_date_dt,
    r.calc_pickup_hour AS calc_pickup_hour_vl,
    r.calc_dropoff_date AS calc_dropoff_date_dt,
    EXTRACT(DAYOFWEEK FROM r.calc_pickup_date) AS calc_pickup_day_of_week_vl,
    FORMAT_DATE('%A', r.calc_pickup_date) AS calc_pickup_day_name_ds,
    TIMESTAMP_DIFF(TIMESTAMP(r.dropoff_datetime), TIMESTAMP(r.pickup_datetime), SECOND) / 60.0 AS calc_duration_minutes_vl,
    -- geo_* fields enrich raw location IDs with TLC zone lookup descriptors.
    pu.borough AS geo_pickup_borough_ds,
    pu.zone AS geo_pickup_zone_ds,
    pu.service_zone AS geo_pickup_service_zone_ds,
    do_zone.borough AS geo_dropoff_borough_ds,
    do_zone.zone AS geo_dropoff_zone_ds,
    do_zone.service_zone AS geo_dropoff_service_zone_ds,
    CASE r.payment_type
      WHEN 1 THEN 'Credit card'
      WHEN 2 THEN 'Cash'
      WHEN 3 THEN 'No charge'
      WHEN 4 THEN 'Dispute'
      WHEN 5 THEN 'Unknown'
      WHEN 6 THEN 'Voided trip'
      ELSE 'Other'
    END AS calc_payment_type_name_ds,
    -- Earnings metrics use total_amount because it approximates gross driver-facing revenue.
    SAFE_DIVIDE(CAST(r.tip_amount AS FLOAT64), NULLIF(CAST(r.fare_amount AS FLOAT64), 0)) AS calc_tip_pct_vl,
    SAFE_DIVIDE(CAST(r.total_amount AS FLOAT64) * 60.0, NULLIF(TIMESTAMP_DIFF(TIMESTAMP(r.dropoff_datetime), TIMESTAMP(r.pickup_datetime), SECOND) / 60.0, 0)) AS calc_earnings_per_hour_vl,
    SAFE_DIVIDE(CAST(r.total_amount AS FLOAT64), NULLIF(r.trip_distance, 0)) AS calc_earnings_per_mile_vl,
    -- Airport IDs are TLC zone IDs for EWR, JFK, and LaGuardia.
    r.pickup_location_id IN (1, 132, 138) AS flag_airport_pickup_lg,
    r.dropoff_location_id IN (1, 132, 138) AS flag_airport_dropoff_lg
  FROM `{project_id}.{bronze_dataset}.yellow_trips_raw` r
  LEFT JOIN `{project_id}.{bronze_dataset}.taxi_zone_lookup` pu
    ON r.pickup_location_id = pu.location_id
  LEFT JOIN `{project_id}.{bronze_dataset}.taxi_zone_lookup` do_zone
    ON r.dropoff_location_id = do_zone.location_id
  WHERE r.meta_source_month = DATE('{source_month}-01')
),
flagged AS (
  SELECT
    *,
    -- Quality flags are conservative business rules for analysis eligibility;
    -- raw records remain preserved in bronze even when excluded from gold marts.
    ARRAY_CONCAT(
      IF(pickup_dt IS NULL OR dropoff_dt IS NULL, ['missing_datetime'], []),
      IF(calc_duration_minutes_vl <= 0, ['non_positive_duration'], []),
      IF(calc_duration_minutes_vl > 0 AND calc_duration_minutes_vl < 2, ['duration_lt_2m'], []),
      IF(calc_duration_minutes_vl > 240, ['duration_gt_4h'], []),
      IF(trip_distance_miles_vl <= 0, ['non_positive_distance'], []),
      IF(trip_distance_miles_vl > 100, ['distance_gt_100mi'], []),
      IF(total_amount_vl <= 0, ['non_positive_total'], []),
      IF(total_amount_vl > 500, ['total_gt_500'], []),
      IF(fare_amount_vl <= 0, ['non_positive_fare'], []),
      IF(calc_tip_pct_vl < 0 OR calc_tip_pct_vl > 1, ['tip_pct_outlier'], []),
      IF(calc_earnings_per_hour_vl > 300, ['earnings_per_hour_gt_300'], []),
      IF(calc_earnings_per_mile_vl > 100, ['earnings_per_mile_gt_100'], []),
      IF(passenger_count_vl IS NULL, ['missing_passenger_count'], []),
      IF(passenger_count_vl = 0, ['zero_passengers'], []),
      IF(geo_pickup_borough_ds IS NULL OR geo_dropoff_borough_ds IS NULL, ['missing_zone_lookup'], []),
      IF(geo_pickup_borough_ds IN ('Unknown', 'N/A') OR geo_dropoff_borough_ds IN ('Unknown', 'N/A'), ['unknown_zone'], [])
    ) AS quality_flag_array
  FROM enriched
)
SELECT
  vendor_id,
  pickup_dt,
  dropoff_dt,
  passenger_count_vl,
  trip_distance_miles_vl,
  ratecode_id,
  store_and_fwd_flag_cd,
  pickup_location_id,
  dropoff_location_id,
  payment_type_id,
  fare_amount_vl,
  extra_amount_vl,
  mta_tax_amount_vl,
  tip_amount_vl,
  tolls_amount_vl,
  improvement_surcharge_amount_vl,
  total_amount_vl,
  congestion_surcharge_amount_vl,
  airport_fee_amount_vl,
  meta_trip_id,
  meta_source_month_dt,
  meta_source_row_number_vl,
  meta_ingested_at_dt,
  calc_pickup_date_dt,
  calc_pickup_hour_vl,
  calc_dropoff_date_dt,
  calc_pickup_day_of_week_vl,
  calc_pickup_day_name_ds,
  calc_duration_minutes_vl,
  geo_pickup_borough_ds,
  geo_pickup_zone_ds,
  geo_pickup_service_zone_ds,
  geo_dropoff_borough_ds,
  geo_dropoff_zone_ds,
  geo_dropoff_service_zone_ds,
  calc_payment_type_name_ds,
  calc_tip_pct_vl,
  calc_earnings_per_hour_vl,
  calc_earnings_per_mile_vl,
  flag_airport_pickup_lg,
  flag_airport_dropoff_lg,
  ARRAY_LENGTH(quality_flag_array) = 0 AS dq_analysis_eligible_lg,
  ARRAY_LENGTH(quality_flag_array) AS dq_issue_count_vl,
  ARRAY_TO_STRING(quality_flag_array, ',') AS dq_flags_ds
FROM flagged;

COMMIT TRANSACTION;
