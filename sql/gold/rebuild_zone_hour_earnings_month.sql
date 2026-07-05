-- Description: Rebuilds monthly pickup zone/hour earning opportunity metrics.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)

BEGIN TRANSACTION;

-- Refresh only the affected month to keep incremental runs cheap.
DELETE FROM `{project_id}.{gold_dataset}.zone_hour_earnings`
WHERE source_month_dt = DATE('{source_month}-01');

INSERT INTO `{project_id}.{gold_dataset}.zone_hour_earnings`
SELECT
  meta_source_month_dt AS source_month_dt,
  geo_pickup_borough_ds AS pickup_borough_ds,
  geo_pickup_zone_ds AS pickup_zone_ds,
  calc_pickup_hour_vl AS pickup_hour_vl,
  COUNT(*) AS trips_vl,
  AVG(CAST(total_amount_vl AS FLOAT64)) AS avg_total_amount_vl,
  AVG(CAST(tip_amount_vl AS FLOAT64)) AS avg_tip_amount_vl,
  AVG(calc_tip_pct_vl) AS avg_tip_pct_vl,
  AVG(trip_distance_miles_vl) AS avg_trip_distance_vl,
  AVG(calc_duration_minutes_vl) AS avg_duration_minutes_vl,
  AVG(calc_earnings_per_hour_vl) AS avg_earnings_per_hour_vl,
  -- Approximate quantiles are sufficient for ranking and cheaper than exact percentiles.
  APPROX_QUANTILES(calc_earnings_per_hour_vl, 100)[OFFSET(50)] AS median_earnings_per_hour_vl,
  APPROX_QUANTILES(calc_earnings_per_hour_vl, 100)[OFFSET(75)] AS p75_earnings_per_hour_vl,
  -- Score balances profitability with observed demand volume.
  AVG(calc_earnings_per_hour_vl) * LOG10(COUNT(*) + 10) AS opportunity_score_vl
FROM `{project_id}.{silver_dataset}.fact_trips`
WHERE meta_source_month_dt = DATE('{source_month}-01')
  AND dq_analysis_eligible_lg
GROUP BY source_month_dt, pickup_borough_ds, pickup_zone_ds, pickup_hour_vl
-- Minimum sample threshold avoids recommending sparse zone/hour combinations.
HAVING trips_vl >= 100;

COMMIT TRANSACTION;
