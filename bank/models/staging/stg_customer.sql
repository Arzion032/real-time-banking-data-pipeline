{{ config(materialized='view') }}

WITH ranked AS (
    SELECT 
        v:id::string AS customer_id,
        v:first_name::string AS first_name,
        v:last_name::string AS last_name,
        v:email::string AS email,
        v:created_at::timestamp AS created_at,
        current_timestamp AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:id::string 
            ORDER BY v:created_at::timestamp DESC
            ) AS rn
    FROM {{ source('raw','customers') }} v
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1;