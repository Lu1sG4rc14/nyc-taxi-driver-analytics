-- Description: Rebuilds monthly tipping patterns by borough, hour, and payment type.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)

BEGIN TRANSACTION;

-- Refresh only the affected month to keep incremental runs cheap.
DELETE FROM `{project_id}.{gold_dataset}.payment_tip_patterns`
WHERE source_month_dt = DATE('{source_month}-01');

INSERT INTO `{project_id}.{gold_dataset}.payment_tip_patterns`
SELECT
  meta_source_month_dt AS source_month_dt,
  geo_pickup_borough_ds AS pickup_borough_ds,
  calc_pickup_hour_vl AS pickup_hour_vl,
  calc_payment_type_name_ds AS payment_type_name_ds,
  COUNT(*) AS trips_vl,
  AVG(CAST(total_amount_vl AS FLOAT64)) AS avg_total_amount_vl,
  AVG(CAST(tip_amount_vl AS FLOAT64)) AS avg_tip_amount_vl,
  AVG(calc_tip_pct_vl) AS avg_tip_pct_vl,
  -- Credit card share helps interpret tip behavior because cash tips may be under-recorded.
  AVG(IF(payment_type_id = 1, 1.0, 0.0)) AS credit_card_share_vl
FROM `{project_id}.{silver_dataset}.fact_trips`
WHERE meta_source_month_dt = DATE('{source_month}-01')
  AND dq_analysis_eligible_lg
GROUP BY source_month_dt, pickup_borough_ds, pickup_hour_vl, payment_type_name_ds
-- Minimum sample threshold avoids presenting noisy payment/tip combinations.
HAVING trips_vl >= 100;

COMMIT TRANSACTION;
