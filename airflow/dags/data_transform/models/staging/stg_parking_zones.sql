{{ config(materialized='view') }}

SELECT
    pz_id,
    district,
    CASE
        WHEN address IS NULL THEN 'Без адреса'
        ELSE address
    END as address,
    is_paid,
    storeys_count,
    max_places
FROM {{ source('raw_layer', 'parking_zones') }}
WHERE storeys_count > 0
    AND max_places BETWEEN 200 AND 5000