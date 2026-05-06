{{ config(materialized='table') }}

SELECT
    ho.pz_id,
    ho.hour_ts,
    ho.occupancy_pct,
    ho.max_places,
    -- lag 1 час
    COALESCE(
        (SELECT ho2.occupancy_pct 
         FROM {{ ref('int_hourly_occupancy') }} ho2
         WHERE ho2.pz_id = ho.pz_id 
           AND ho2.hour_ts = ho.hour_ts - INTERVAL '1 hour'
         LIMIT 1), 0
    ) AS lag_1h,
    -- lag 24 часа
    COALESCE(
        (SELECT ho2.occupancy_pct 
         FROM {{ ref('int_hourly_occupancy') }} ho2
         WHERE ho2.pz_id = ho.pz_id 
           AND ho2.hour_ts = ho.hour_ts - INTERVAL '24 hours'
         LIMIT 1), 0
    ) AS lag_24h,
    -- lag 168 часов (неделя)
    COALESCE(
        (SELECT ho2.occupancy_pct 
         FROM {{ ref('int_hourly_occupancy') }} ho2
         WHERE ho2.pz_id = ho.pz_id 
           AND ho2.hour_ts = ho.hour_ts - INTERVAL '168 hours'
         LIMIT 1), 0
    ) AS lag_168h,
    -- rolling average за 7 дней для этого же часа
    COALESCE(
        (SELECT AVG(ho2.occupancy_pct)
         FROM {{ ref('int_hourly_occupancy') }} ho2
         WHERE ho2.pz_id = ho.pz_id
           AND ho2.hour_ts BETWEEN ho.hour_ts - INTERVAL '7 days' AND ho.hour_ts
           AND EXTRACT(HOUR FROM ho2.hour_ts) = EXTRACT(HOUR FROM ho.hour_ts)
        ), ho.occupancy_pct
    ) AS rolling_avg_7d,
    EXTRACT(HOUR FROM ho.hour_ts) AS hour,
    EXTRACT(DOW FROM ho.hour_ts) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM ho.hour_ts) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(MONTH FROM ho.hour_ts) AS month
FROM {{ ref('int_hourly_occupancy') }} ho
WHERE ho.occupancy_pct IS NOT NULL