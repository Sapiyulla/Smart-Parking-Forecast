{{ config(materialized='table') }}

WITH 
    delta_per_hour AS (
        SELECT
            en.pz_id,
            en.hour,
            en.entrance_count - COALESCE(ex.exit_count, 0) AS delta
        FROM (
            SELECT pz_id, DATE_TRUNC('hour', ts) AS hour, COUNT(*) AS entrance_count
            FROM {{ ref('stg_parkings') }}
            WHERE action = 'entrance'
            GROUP BY pz_id, hour
        ) en
        LEFT JOIN (
            SELECT pz_id, DATE_TRUNC('hour', ts) AS hour, COUNT(*) AS exit_count
            FROM {{ ref('stg_parkings') }}
            WHERE action = 'exit'
            GROUP BY pz_id, hour
        ) ex
        ON en.hour = ex.hour AND en.pz_id = ex.pz_id
    ),

    cumulative AS (
        SELECT
            d1.pz_id,
            d1.hour,
            (SELECT SUM(d2.delta) 
             FROM delta_per_hour d2 
             WHERE d2.pz_id = d1.pz_id AND d2.hour <= d1.hour) AS occupied
        FROM delta_per_hour d1
    )

SELECT
    c.pz_id,
    c.hour AS hour_ts,
    c.occupied,
    pz.max_places,
    GREATEST(0, LEAST(100, ROUND((c.occupied::numeric / pz.max_places) * 100, 2))) AS occupancy_pct
FROM cumulative c
INNER JOIN {{ ref('stg_parking_zones') }} pz
    ON c.pz_id = pz.pz_id