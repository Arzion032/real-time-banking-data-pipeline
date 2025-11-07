{{ config(materialized='view') }}

SELECT
    v:id::string                 AS id,
    v:transaction_id::string     AS transaction_id,
    v:account_id::string         AS account_id,
    v:entry_type::string         AS entry_type,
    v:amount::float              AS amount,
    v:created_at::timestamp      AS txn_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'ledger_entries') }}