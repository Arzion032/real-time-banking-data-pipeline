{{ config(materialized='view') }}

SELECT
    v:id::string                 AS transaction_id,
    v:account_id::string         AS account_id,
    v:related_account_id::string AS related_account_id,
    v:txn_type::string           AS txn_type,
    v:direction::string          AS direction,
    v:amount::float              AS amount,
    v:balance_after::string      AS balance_after,
    v:status::string             AS txn_status,
    v:reference::string         AS reference,
    v:description::string         AS txn_desc,
    v:created_at::timestamp      AS txn_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'transactions') }}