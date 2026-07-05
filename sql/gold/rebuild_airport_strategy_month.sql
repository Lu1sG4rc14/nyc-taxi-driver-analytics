BEGIN TRANSACTION;

DELETE FROM `{project_id}.{gold_dataset}.airport_strategy`
WHERE source_month_dt = DATE('{source_month}-01');

INSERT INTO `{project_id}.{gold_dataset}.airport_strategy`
SELECT
  meta_source_month_dt AS source_month_dt,
  calc_pickup_hour_vl AS pickup_hour_vl,
  geo_pickup_zone_ds AS pickup_zone_ds,
  flag_airport_pickup_lg,
  flag_airport_dropoff_lg,
  COUNT(*) AS trips_vl,
  AVG(CAST(total_amount_vl AS FLOAT64)) AS avg_total_amount_vl,
  AVG(calc_duration_minutes_vl) AS avg_duration_minutes_vl,
  AVG(calc_earnings_per_hour_vl) AS avg_earnings_per_hour_vl,
  AVG(calc_tip_pct_vl) AS avg_tip_pct_vl
FROM `{project_id}.{silver_dataset}.fact_trips`
WHERE meta_source_month_dt = DATE('{source_month}-01')
  AND dq_analysis_eligible_lg
  AND (flag_airport_pickup_lg OR flag_airport_dropoff_lg)
GROUP BY source_month_dt, pickup_hour_vl, pickup_zone_ds, flag_airport_pickup_lg, flag_airport_dropoff_lg
HAVING trips_vl >= 25;

COMMIT TRANSACTION;
