{{ config(materialized='view') }}

SELECT 
    id,
    pz_id,
    storey,
    action,
    CASE
        WHEN action = 'entrance' THEN rate
        ELSE 0
    END as rate,
    ts
FROM {{ source('raw_layer', 'parkings') }}
WHERE storey > 0
    AND ts IS NOT NULL