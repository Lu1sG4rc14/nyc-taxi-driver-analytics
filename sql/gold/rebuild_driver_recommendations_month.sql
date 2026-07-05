BEGIN TRANSACTION;

DELETE FROM `{project_id}.{gold_dataset}.driver_recommendations`
WHERE source_month_dt = DATE('{source_month}-01');

INSERT INTO `{project_id}.{gold_dataset}.driver_recommendations`
WITH zone_hour AS (
  SELECT
    source_month_dt,
    ROW_NUMBER() OVER (ORDER BY opportunity_score_vl DESC) AS recommendation_rank_vl,
    'zone_hour' AS recommendation_type_ds,
    FORMAT(
      'Prioritize %s around %02d:00',
      pickup_zone_ds,
      pickup_hour_vl
    ) AS advice_ds,
    FORMAT(
      '%d eligible trips, avg $%.2f/hour, median $%.2f/hour, avg trip $%.2f',
      trips_vl,
      avg_earnings_per_hour_vl,
      median_earnings_per_hour_vl,
      avg_total_amount_vl
    ) AS evidence_ds,
    opportunity_score_vl
  FROM `{project_id}.{gold_dataset}.zone_hour_earnings`
  WHERE source_month_dt = DATE('{source_month}-01')
  QUALIFY recommendation_rank_vl <= 10
),
airport AS (
  SELECT
    source_month_dt,
    ROW_NUMBER() OVER (ORDER BY avg_earnings_per_hour_vl * LOG10(trips_vl + 10) DESC) AS recommendation_rank_vl,
    'airport' AS recommendation_type_ds,
    FORMAT(
      'Watch %s airport-related demand around %02d:00',
      pickup_zone_ds,
      pickup_hour_vl
    ) AS advice_ds,
    FORMAT(
      '%d eligible airport trips, avg $%.2f/hour, avg duration %.1f minutes',
      trips_vl,
      avg_earnings_per_hour_vl,
      avg_duration_minutes_vl
    ) AS evidence_ds,
    avg_earnings_per_hour_vl * LOG10(trips_vl + 10) AS opportunity_score_vl
  FROM `{project_id}.{gold_dataset}.airport_strategy`
  WHERE source_month_dt = DATE('{source_month}-01')
  QUALIFY recommendation_rank_vl <= 5
),
tips AS (
  SELECT
    source_month_dt,
    ROW_NUMBER() OVER (ORDER BY avg_tip_pct_vl DESC, trips_vl DESC) AS recommendation_rank_vl,
    'payment_tips' AS recommendation_type_ds,
    FORMAT(
      'Favor %s rides in %s around %02d:00 when possible',
      payment_type_name_ds,
      pickup_borough_ds,
      pickup_hour_vl
    ) AS advice_ds,
    FORMAT(
      '%d eligible trips, avg tip %.1f%%, avg tip $%.2f',
      trips_vl,
      avg_tip_pct_vl * 100,
      avg_tip_amount_vl
    ) AS evidence_ds,
    avg_tip_pct_vl * LOG10(trips_vl + 10) AS opportunity_score_vl
  FROM `{project_id}.{gold_dataset}.payment_tip_patterns`
  WHERE source_month_dt = DATE('{source_month}-01')
    AND avg_tip_pct_vl IS NOT NULL
  QUALIFY recommendation_rank_vl <= 5
)
SELECT * FROM zone_hour
UNION ALL
SELECT * FROM airport
UNION ALL
SELECT * FROM tips;

COMMIT TRANSACTION;
