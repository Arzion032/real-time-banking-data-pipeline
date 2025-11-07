{{ config(materialized='table') }}

WITH latest AS (
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
        dbt_valid_from AS effective_from,
        dbt_valid_to AS effective_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
FROM {{ref('customers_snapshot')}} 
)
SELECT * FROM latest