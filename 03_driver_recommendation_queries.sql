-- Description: Analytical SQL blocks supporting driver recommendations for January 2023.
-- Created: 2026-07-05
-- Author: Luis G (https://github.com/Lu1sG4rc14)
--
-- BigQuery Standard SQL.
-- Run these queries in the GCP project that contains the deployed datasets.
-- Each block is intentionally self-contained and uses only the January 2023
-- Parquet file referenced in the instructions.

-- 01. Recommendation: start with the highest opportunity pickup zone-hours.
-- This ranks pickup zone/hour combinations by the gold opportunity score, which
-- balances earnings per hour with demand volume instead of looking only at fare.
SELECT
  ROW_NUMBER() OVER (ORDER BY opportunity_score_vl DESC) AS rank_vl,
  pickup_borough_ds,
  pickup_zone_ds,
  pickup_hour_vl,
  trips_vl,
  ROUND(avg_earnings_per_hour_vl, 2) AS avg_earnings_per_hour_vl,
  ROUND(median_earnings_per_hour_vl, 2) AS median_earnings_per_hour_vl,
  ROUND(opportunity_score_vl, 2) AS opportunity_score_vl
FROM `L30_gold.zone_hour_earnings`
WHERE source_month_dt = DATE '2023-01-01'
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 02. Recommendation: challenge the assumption that Manhattan is always best.
-- This compares non-Manhattan zone-hours against the Manhattan monthly average
-- and surfaces high-earning exceptions that a broad borough strategy would miss.
WITH manhattan_benchmark AS (
  SELECT AVG(avg_earnings_per_hour_vl) AS manhattan_avg_earnings_per_hour_vl
  FROM `L30_gold.zone_hour_earnings`
  WHERE source_month_dt = DATE '2023-01-01'
    AND pickup_borough_ds = 'Manhattan'
)
SELECT
  ROW_NUMBER() OVER (
    ORDER BY z.avg_earnings_per_hour_vl - b.manhattan_avg_earnings_per_hour_vl DESC
  ) AS rank_vl,
  z.pickup_borough_ds,
  z.pickup_zone_ds,
  z.pickup_hour_vl,
  z.trips_vl,
  ROUND(z.avg_earnings_per_hour_vl, 2) AS avg_earnings_per_hour_vl,
  ROUND(b.manhattan_avg_earnings_per_hour_vl, 2) AS manhattan_avg_earnings_per_hour_vl,
  ROUND(z.avg_earnings_per_hour_vl - b.manhattan_avg_earnings_per_hour_vl, 2) AS earnings_gap_vs_manhattan_vl
FROM `L30_gold.zone_hour_earnings` z
CROSS JOIN manhattan_benchmark b
WHERE z.source_month_dt = DATE '2023-01-01'
  AND z.pickup_borough_ds != 'Manhattan'
  AND z.trips_vl >= 100
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 03. Recommendation: treat airports as time-window plays.
-- This ranks airport-related pickup windows by earnings per hour weighted by
-- trip volume, avoiding a simplistic "airports are always good" conclusion.
SELECT
  ROW_NUMBER() OVER (
    ORDER BY avg_earnings_per_hour_vl * LOG10(trips_vl + 10) DESC
  ) AS rank_vl,
  pickup_zone_ds,
  pickup_hour_vl,
  flag_airport_pickup_lg,
  flag_airport_dropoff_lg,
  trips_vl,
  ROUND(avg_earnings_per_hour_vl, 2) AS avg_earnings_per_hour_vl,
  ROUND(avg_duration_minutes_vl, 1) AS avg_duration_minutes_vl,
  ROUND(avg_earnings_per_hour_vl * LOG10(trips_vl + 10), 2) AS airport_window_score_vl
FROM `L30_gold.airport_strategy`
WHERE source_month_dt = DATE '2023-01-01'
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 04. Recommendation: use payment context to identify tip upside.
-- This highlights where credit-card rides show the strongest observed tip
-- percentages, filtered to meaningful volumes to avoid tiny-sample artefacts.
SELECT
  ROW_NUMBER() OVER (ORDER BY avg_tip_pct_vl DESC, trips_vl DESC) AS rank_vl,
  payment_type_name_ds,
  pickup_borough_ds,
  pickup_hour_vl,
  trips_vl,
  ROUND(avg_tip_pct_vl * 100, 1) AS avg_tip_pct_vl,
  ROUND(avg_total_amount_vl, 2) AS avg_total_amount_vl,
  ROUND(credit_card_share_vl * 100, 1) AS credit_card_share_vl
FROM `L30_gold.payment_tip_patterns`
WHERE source_month_dt = DATE '2023-01-01'
  AND payment_type_name_ds = 'Credit card'
  AND trips_vl >= 500
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 05. Avoiding pattern: do not maximize gross fare when duration destroys hourly earnings.
-- This surfaces zone-hours with above-average ticket size but below-median hourly
-- earnings, a pattern that can look attractive until time is considered.
WITH month_baseline AS (
  SELECT
    AVG(avg_total_amount_vl) AS avg_total_amount_vl,
    APPROX_QUANTILES(avg_earnings_per_hour_vl, 100)[OFFSET(50)] AS median_earnings_per_hour_vl
  FROM `L30_gold.zone_hour_earnings`
  WHERE source_month_dt = DATE '2023-01-01'
)
SELECT
  ROW_NUMBER() OVER (
    ORDER BY z.avg_total_amount_vl DESC, z.avg_duration_minutes_vl DESC
  ) AS rank_vl,
  z.pickup_borough_ds,
  z.pickup_zone_ds,
  z.pickup_hour_vl,
  z.trips_vl,
  ROUND(z.avg_total_amount_vl, 2) AS avg_total_amount_vl,
  ROUND(z.avg_earnings_per_hour_vl, 2) AS avg_earnings_per_hour_vl,
  ROUND(z.avg_duration_minutes_vl, 1) AS avg_duration_minutes_vl
FROM `L30_gold.zone_hour_earnings` z
CROSS JOIN month_baseline b
WHERE z.source_month_dt = DATE '2023-01-01'
  AND z.trips_vl >= 100
  AND z.avg_total_amount_vl > b.avg_total_amount_vl
  AND z.avg_earnings_per_hour_vl < b.median_earnings_per_hour_vl
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 06. Avoiding pattern: do not assume high airport demand means top profitability.
-- This finds airport windows with meaningful demand but earnings below the best
-- city zone-hour benchmark.
WITH top_zone_hour AS (
  SELECT avg_earnings_per_hour_vl AS top_zone_earnings_per_hour_vl
  FROM `L30_gold.zone_hour_earnings`
  WHERE source_month_dt = DATE '2023-01-01'
  ORDER BY opportunity_score_vl DESC
  LIMIT 1
)
SELECT
  ROW_NUMBER() OVER (ORDER BY a.trips_vl DESC) AS rank_vl,
  a.pickup_zone_ds,
  a.pickup_hour_vl,
  a.flag_airport_pickup_lg,
  a.flag_airport_dropoff_lg,
  a.trips_vl,
  ROUND(a.avg_earnings_per_hour_vl, 2) AS avg_earnings_per_hour_vl,
  ROUND(t.top_zone_earnings_per_hour_vl, 2) AS top_zone_earnings_per_hour_vl,
  ROUND(t.top_zone_earnings_per_hour_vl - a.avg_earnings_per_hour_vl, 2) AS earnings_gap_to_best_zone_hour_vl
FROM `L30_gold.airport_strategy` a
CROSS JOIN top_zone_hour t
WHERE a.source_month_dt = DATE '2023-01-01'
  AND a.trips_vl >= 1000
  AND a.avg_earnings_per_hour_vl < t.top_zone_earnings_per_hour_vl
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 07. Avoiding pattern: do not interpret cash tips as complete tip behavior.
-- Cash gratuities are structurally under-observed in the source, so this compares
-- cash and credit-card tip patterns for the same borough/hour slices.
SELECT
  ROW_NUMBER() OVER (ORDER BY cash.trips_vl DESC) AS rank_vl,
  cash.pickup_borough_ds,
  cash.pickup_hour_vl,
  cash.trips_vl AS cash_trips_vl,
  ROUND(cash.avg_tip_pct_vl * 100, 1) AS cash_avg_tip_pct_vl,
  credit.trips_vl AS credit_card_trips_vl,
  ROUND(credit.avg_tip_pct_vl * 100, 1) AS credit_card_avg_tip_pct_vl,
  ROUND((credit.avg_tip_pct_vl - cash.avg_tip_pct_vl) * 100, 1) AS observed_tip_gap_pct_points_vl
FROM `L30_gold.payment_tip_patterns` cash
JOIN `L30_gold.payment_tip_patterns` credit
  ON cash.source_month_dt = credit.source_month_dt
  AND cash.pickup_borough_ds = credit.pickup_borough_ds
  AND cash.pickup_hour_vl = credit.pickup_hour_vl
WHERE cash.source_month_dt = DATE '2023-01-01'
  AND cash.payment_type_name_ds = 'Cash'
  AND credit.payment_type_name_ds = 'Credit card'
  AND cash.trips_vl >= 1000
  AND credit.trips_vl >= 1000
QUALIFY rank_vl <= 10
ORDER BY rank_vl;

-- 08. Avoiding pattern: do not build advice from unfiltered raw trips.
-- This quantifies excluded records and ranks the most common quality flags, proving
-- why the silver eligibility filter is required before building recommendations.
WITH quality_summary AS (
  SELECT
    COUNT(*) AS all_trips_vl,
    COUNTIF(NOT dq_analysis_eligible_lg) AS excluded_trips_vl,
    SAFE_DIVIDE(COUNTIF(NOT dq_analysis_eligible_lg), COUNT(*)) AS excluded_trip_pct_vl
  FROM `L20_silver.fact_trips`
  WHERE meta_source_month_dt = DATE '2023-01-01'
),
quality_flags AS (
  SELECT
    flag AS dq_flag_ds,
    COUNT(*) AS flagged_trips_vl
  FROM `L20_silver.fact_trips`,
  UNNEST(SPLIT(COALESCE(dq_flags_ds, ''), ',')) AS flag
  WHERE meta_source_month_dt = DATE '2023-01-01'
    AND flag != ''
  GROUP BY dq_flag_ds
)
SELECT
  ROW_NUMBER() OVER (ORDER BY f.flagged_trips_vl DESC) AS rank_vl,
  q.all_trips_vl,
  q.excluded_trips_vl,
  ROUND(q.excluded_trip_pct_vl * 100, 1) AS excluded_trip_pct_vl,
  f.dq_flag_ds,
  f.flagged_trips_vl
FROM quality_flags f
CROSS JOIN quality_summary q
QUALIFY rank_vl <= 10
ORDER BY rank_vl;
