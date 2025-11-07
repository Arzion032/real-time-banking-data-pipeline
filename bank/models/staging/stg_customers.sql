{{ config(materialized='view') }}

WITH ranked AS (
    SELECT 
        v:id::string AS customer_id,
        v:first_name::string AS first_name,
        v:last_name::string AS last_name,
        v:email::string AS email,
        v:phone::string AS phone,
        v:date_of_birth::date AS date_of_birth,
        v:address::string AS cust_address,
        v:status::string AS cust_status,
        v:created_at::timestamp AS created_at,
        CURRENT_TIMESTAMP AS load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY v:id::string 
            ORDER BY v:created_at::timestamp DESC
            ) AS rn
    FROM {{ source('raw','customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    cust_address,
    cust_status,
    created_at,
    load_timestamp
FROM ranked
WHERE rn = 1