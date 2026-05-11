{{ config(materialized='table') }}

SELECT
    pz_id,
    hour_ts,
    occupancy_pct,
    lag_1h,
    lag_24h,
    lag_168h,
    rolling_avg_7d,
    hour,
    day_of_week,
    is_weekend,
    month
FROM {{ ref('int_daily_metrics') }}
WHERE occupancy_pct IS NOT NULL